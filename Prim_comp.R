library(readr)
library(ggplot2)
library(dplyr)
library(tidyr)
library(data.table)
library(rpart)
library(R.utils)
library(rlist)
library(parallel)
library(lightgbm)

#Limpieza del entorno de programacion
rm(list = ls()) 
dev.off()


#Determinar el directorio de trabajo
setwd("/Users/martinleon/Desktop/Academicos/MAESTRIA_CD_FCEN/Especializacion/Data_mining_Economia_y_Finanzas/Primera_competencia/R/data")

#Dataset
dataset <- fread("competencia_01_clases.csv", header = T)


dataset <- dataset %>%
  mutate(
    foto_mes = case_when(
      foto_mes == 202105 & clase_ternaria == "BAJA+1" ~ 2021052,
      TRUE ~ foto_mes
    )
  )

format(Sys.time(), "%a %b %d %X %Y")


#Optimizacion de hiperparametros
#cargo las librerias que necesito

# if( !require("primes") ) install.packages("primes")
# require("primes")
# 
# if( !require("utils") ) install.packages("utils")
# require("utils")
# 
# if( !require("rlist") ) install.packages("rlist")
# require("rlist")
# 
# if( !require("yaml")) install.packages("yaml")
# require("yaml")
# 
# if( !require("lightgbm") ) install.packages("lightgbm")
# require("lightgbm")
# 
# if( !require("DiceKriging") ) install.packages("DiceKriging")
# require("DiceKriging")
# 
# if( !require("mlrMBO") ) install.packages("mlrMBO")
# require("mlrMBO")


#Definicion de parametros

PARAM <- list()
PARAM$experimento <- "E0053"
PARAM$semilla_primigenia <- 87654321
# Primera es: 100169, Luego: 100183, 100189, 100193, 100213, 42

# training y future
PARAM$train <- c(202101, 202102, 202103,202104, 2021052)
PARAM$train_final <- c(202101, 202102, 202103,202104, 2021052)
PARAM$future <- c(202106)
PARAM$semilla_kaggle <- 314159
PARAM$cortes <- seq(2000, 25000, by= 500)

# un undersampling de 0.1  toma solo el 10% de los CONTINUA
# undersampling de 1.0  implica tomar TODOS los datos

PARAM$trainingstrategy$undersampling <- 0.5


# Parametros LightGBM

PARAM$hyperparametertuning$xval_folds <- 5

# parametros fijos del LightGBM que se pisaran con la parte variable de la BO
PARAM$lgbm$param_fijos <-  list(
  boosting= "gbdt", # puede ir  dart  , ni pruebe random_forest
  objective= "binary",
  metric= "auc",
  first_metric_only= FALSE,
  boost_from_average= TRUE,
  feature_pre_filter= FALSE,
  force_row_wise= TRUE, # para reducir warnings
  verbosity= -100,
  
  seed= PARAM$semilla_primigenia,
  
  max_depth= -1L, # -1 significa no limitar,  por ahora lo dejo fijo
  min_gain_to_split= 0.1, # min_gain_to_split >= 0
  min_sum_hessian_in_leaf= 0.001, #  min_sum_hessian_in_leaf >= 0.0
  lambda_l1= 0.8, # lambda_l1 >= 0.0
  lambda_l2= 0.8, # lambda_l2 >= 0.0
  max_bin= 31L, # lo debo dejar fijo, no participa de la BO
  
  bagging_fraction= 1.0, # 0.0 < bagging_fraction <= 1.0
  pos_bagging_fraction= 1.0, # 0.0 < pos_bagging_fraction <= 1.0
  neg_bagging_fraction= 1.0, # 0.0 < neg_bagging_fraction <= 1.0
  is_unbalance= FALSE, #
  scale_pos_weight= 1.0, # scale_pos_weight > 0.0
  
  drop_rate= 0.1, # 0.0 < neg_bagging_fraction <= 1.0
  max_drop= 50, # <=0 means no limit
  skip_drop= 0.5, # 0.0 <= skip_drop <= 1.0
  
  extra_trees= FALSE,
  
  num_iterations= 2000,
  learning_rate= 0.05,
  feature_fraction= 0.5,
  num_leaves= 200,
  min_data_in_leaf= 150
)





# Aqui se cargan los bordes de los hiperparametros de la BO
PARAM$hypeparametertuning$hs <- makeParamSet(
  makeIntegerParam("num_iterations", lower= 8L, upper= 2048L),
  makeNumericParam("learning_rate", lower= 0.01, upper= 0.3),
  makeNumericParam("feature_fraction", lower= 0.1, upper= 1.0),
  makeIntegerParam("num_leaves", lower= 8L, upper= 2048L),
  makeIntegerParam("min_data_in_leaf", lower= 1L, upper= 8000L)
)

PARAM$hyperparametertuning$iteraciones <- 5 # iteraciones bayesianas



# particionar agrega una columna llamada fold a un dataset
#   que consiste en una particion estratificada segun agrupa
# particionar( data=dataset, division=c(70,30),
#  agrupa=clase_ternaria, seed=semilla)   crea una particion 70, 30

particionar <- function(data, division, agrupa= "", campo= "fold", start= 1, seed= NA) {
  if (!is.na(seed)) set.seed(seed, "L'Ecuyer-CMRG")
  
  bloque <- unlist(mapply(
    function(x, y) {rep(y, x)},division, seq(from= start, length.out= length(division))))
  
  data[, (campo) := sample(rep(bloque,ceiling(.N / length(bloque))))[1:.N],by= agrupa]
}



# iniciliazo el dataset de realidad, para medir ganancia
realidad_inicializar <- function( pfuture, pparam) {
  
  # datos para verificar la ganancia
  drealidad <- pfuture[, list(numero_de_cliente, foto_mes, clase_ternaria)]
  
  particionar(drealidad,
              division= c(3, 7),
              agrupa= "clase_ternaria",
              seed= PARAM$semilla_kaggle
  )
  
  return( drealidad )
}




# evaluo ganancia en los datos de la realidad

realidad_evaluar <- function( prealidad, pprediccion) {
  
  prealidad[ pprediccion,
             on= c("numero_de_cliente", "foto_mes"),
             predicted:= i.Predicted
  ]
  
  tbl <- prealidad[, list("qty"=.N), list(fold, predicted, clase_ternaria)]
  
  res <- list()
  res$public  <- tbl[fold==1 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.3
  res$private <- tbl[fold==2 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.7
  res$total <- tbl[predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]
  
  prealidad[, predicted:=NULL]
  return( res )
}


#Preprocesamiento
dataset_train <- dataset[foto_mes %in% PARAM$train]
# paso la clase a binaria que tome valores {0,1}  enteros
#  BAJA+1 y BAJA+2  son  1,   CONTINUA es 0
#  a partir de ahora ya NO puedo cortar  por prob(BAJA+2) > 1/40

dataset_train[,
              clase01 := ifelse(clase_ternaria %in% c("BAJA+2","BAJA+1"), 1L, 0L)
]


# defino los datos que forma parte del training
# aqui se hace el undersampling de los CONTINUA
# notar que para esto utilizo la SEGUNDA semilla

set.seed(PARAM$semilla_primigenia, kind = "L'Ecuyer-CMRG")
dataset_train[, azar := runif(nrow(dataset_train))]
dataset_train[, training := 0L]

dataset_train[
  foto_mes %in%  PARAM$train &
    (azar <= PARAM$trainingstrategy$undersampling | clase_ternaria %in% c("BAJA+1", "BAJA+2")),
  training := 1L
]


# los campos que se van a utilizar

campos_buenos <- setdiff(
  colnames(dataset_train),
  c("clase_ternaria", "clase01", "azar", "training")
)



# dejo los datos en el formato que necesita LightGBM

dtrain <- lgb.Dataset(
  data= data.matrix(dataset_train[training == 1L, campos_buenos, with= FALSE]),
  label= dataset_train[training == 1L, clase01],
  free_raw_data= FALSE
)



#Configuracion de la optimizacion bayesiana
# En el argumento x llegan los parmaetros de la bayesiana
#  devuelve la AUC en cross validation del modelo entrenado

EstimarGanancia_AUC_lightgbm <- function(x) {
  
  # x pisa (o agrega) a param_fijos
  param_completo <- modifyList(PARAM$lgbm$param_fijos, x)
  
  # entreno LightGBM
  modelocv <- lgb.cv(
    data= dtrain,
    nfold= PARAM$hyperparametertuning$xval_folds,
    stratified= TRUE,
    param= param_completo
  )
  
  # obtengo la ganancia
  AUC <- modelocv$best_score
  
  # hago espacio en la memoria
  rm(modelocv)
  gc(full= TRUE, verbose= FALSE)
  
  message(format(Sys.time(), "%a %b %d %X %Y"), " AUC ", AUC)
  
  return(AUC)
}




# Aqui comienza la configuracion de la Bayesian Optimization

# en este archivo quedan la evolucion binaria de la BO
kbayesiana <- "bayesiana.RDATA"

funcion_optimizar <- EstimarGanancia_AUC_lightgbm # la funcion que voy a maximizar

configureMlr(show.learner.output= FALSE)

# configuro la busqueda bayesiana,  los hiperparametros que se van a optimizar
# por favor, no desesperarse por lo complejo

obj.fun <- makeSingleObjectiveFunction(
  fn= funcion_optimizar, # la funcion que voy a maximizar
  minimize= FALSE, # estoy Maximizando la ganancia
  noisy= TRUE,
  par.set= PARAM$hypeparametertuning$hs, # definido al comienzo del programa
  has.simple.signature= FALSE # paso los parametros en una lista
)

# cada 600 segundos guardo el resultado intermedio
ctrl <- makeMBOControl(
  save.on.disk.at.time= 600, # se graba cada 600 segundos
  save.file.path= kbayesiana
) # se graba cada 600 segundos

# indico la cantidad de iteraciones que va a tener la Bayesian Optimization
ctrl <- setMBOControlTermination(
  ctrl,
  iters= PARAM$hyperparametertuning$iteraciones
) # cantidad de iteraciones

# defino el método estandar para la creacion de los puntos iniciales,
# los "No Inteligentes"
ctrl <- setMBOControlInfill(ctrl, crit= makeMBOInfillCritEI())

# establezco la funcion que busca el maximo
surr.km <- makeLearner(
  "regr.km",
  predict.type= "se",
  covtype= "matern3_2",
  control= list(trace= TRUE)
)




#Corrida de la optimizacion bayesiana!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# inicio la optimizacion bayesiana, retomando si ya existe
# es la celda mas lenta de todo el notebook

if (!file.exists(kbayesiana)) {
  bayesiana_salida <- mbo(obj.fun, learner= surr.km, control= ctrl)
} else {
  bayesiana_salida <- mboContinue(kbayesiana) # retomo en caso que ya exista
}



tb_bayesiana <- as.data.table(bayesiana_salida$opt.path)


# almaceno los resultados de la Bayesian Optimization
# y capturo los mejores hiperparametros encontrados

tb_bayesiana <- as.data.table(bayesiana_salida$opt.path)

tb_bayesiana[, iter := .I]

# ordeno en forma descendente por AUC = y
setorder(tb_bayesiana, -y)

# grabo para eventualmente poder utilizarlos en OTRA corrida
fwrite( tb_bayesiana,
        file= "BO_log.txt",
        sep= "\t"
)

# los mejores hiperparámetros son los que quedaron en el registro 1 de la tabla
PARAM$out$lgbm$mejores_hiperparametros <- tb_bayesiana[
  1, # el primero es el de mejor AUC
  setdiff(colnames(tb_bayesiana),
          c("y","dob","eol","error.message","exec.time","ei","error.model",
            "train.time","prop.type","propose.time","se","mean","iter")),
  with= FALSE
]


PARAM$out$lgbm$y <- tb_bayesiana[1, y]



write_yaml( PARAM, file="PARAM_E0053.yml")



#Produccion
#Final training
# clase01
dataset[, clase01 := ifelse(clase_ternaria %in% c("BAJA+1", "BAJA+2"), 1L, 0L)]

dataset_train <- dataset[foto_mes %in% PARAM$train_final]
dataset_train[,.N,clase_ternaria]


# dejo los datos en el formato que necesita LightGBM

dtrain_final <- lgb.Dataset(
  data= data.matrix(dataset_train[, campos_buenos, with= FALSE]),
  label= dataset_train[, clase01]
)


param_final <- modifyList(PARAM$lgbm$param_fijos,
                          PARAM$out$lgbm$mejores_hiperparametros)


#Genero modelo final
# este punto es muy SUTIL  y será revisado en la Clase 05

param_normalizado <- copy(param_final)
param_normalizado$min_data_in_leaf <-  round(param_final$min_data_in_leaf / PARAM$trainingstrategy$undersampling)


# entreno LightGBM

modelo_final <- lgb.train(
  data= dtrain_final,
  param= param_normalizado
)

# ahora imprimo la importancia de variables

tb_importancia <- as.data.table(lgb.importance(modelo_final))
archivo_importancia <- "impoE0053.txt"

fwrite(tb_importancia,
       file= archivo_importancia,
       sep= "\t"
)



# grabo a disco el modelo en un formato para seres humanos ... ponele ...
lgb.save(modelo_final, "modeloE0053.txt" )



#Scoring
# aplico el modelo a los datos sin clase
dfuture <- dataset[foto_mes %in% PARAM$future]

# aplico el modelo a los datos nuevos
prediccion <- predict(
  modelo_final,
  data.matrix(dfuture[, campos_buenos, with= FALSE])
)




# inicilizo el dataset  drealidad
drealidad <- realidad_inicializar( dfuture, PARAM)


# tabla de prediccion

tb_prediccion <- dfuture[, list(numero_de_cliente, foto_mes)]
tb_prediccion[, prob := prediccion ]

# grabo las probabilidad del modelo
fwrite(tb_prediccion,
       file= "prediccionE0053.txt",
       sep= "\t"
)



# genero archivos con los  "envios" mejores
# suba TODOS los archivos a Kaggle

# ordeno por probabilidad descendente
setorder(tb_prediccion, -prob)

dir.create("kaggle")

for (envios in PARAM$cortes) {
  
  tb_prediccion[, Predicted := 0L] # seteo inicial a 0
  tb_prediccion[1:envios, Predicted := 1L] # marco los primeros
  
  archivo_kaggle <- paste0("./kaggle/KA", PARAM$experimento, "_", envios, ".csv")
  
  # grabo el archivo
  fwrite(tb_prediccion[, list(numero_de_cliente, Predicted)],
         file= archivo_kaggle,
         sep= ","
  )
  
  res <- realidad_evaluar( drealidad, tb_prediccion)
  
  options(scipen = 999)
  cat( "Envios=", envios, "\t",
       " TOTAL=", res$total,
       "  Public=", res$public,
       " Private=", res$private,
       "\n",
       sep= ""
  )
  
}


write_yaml( PARAM, file="PARAM_E0053.yml")

format(Sys.time(), "%a %b %d %X %Y")


