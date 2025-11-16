# -------------------------------------------------------------------
# -------------------------------------------------------------------
# ----------------------SEGUNDA COMPETENCIA--------------------------
# ----------------------ENTREGA--------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# Preparacion del entorno y codigo
# -------------------------------------------------------------------


# limpio la memoria
Sys.time()
rm(list=ls(all.names=TRUE)) # remove all objects
gc(full=TRUE, verbose=FALSE) # garbage collection


require( "data.table" )

# Cargar dataset comprimido
dataset <- fread("competencia_02_crudo.csv.gz")

# -------------------------------------------------------------------
# Pre procesamiento
# -------------------------------------------------------------------

# Eliminacion de atributos problematicos
dataset[, mprestamos_personales := NULL ]
dataset[, cprestamos_personales := NULL ]

# Imputo nulls en variables 0 de pandemia
# 201904 - ctarjeta_visa_debitos_automaticos, mttarjeta_visa_debitos_automaticos
dataset[foto_mes == 201904, c("ctarjeta_visa_debitos_automaticos", "mttarjeta_visa_debitos_automaticos") := NA]

# 201905 - mrentabilidad, mrentabilidad_annual, mcomisiones
dataset[foto_mes == 201905, c("mrentabilidad", "mrentabilidad_annual", "mcomisiones", "mactivos_margen", "mpasivos_margen", "ccomisiones_otras", "mcomisiones_otras") := NA]

# 201910 - mrentabilidad, mrentabilidad_annual, mcomisiones

dataset[foto_mes == 201910, c("mrentabilidad", "mrentabilidad_annual", "mcomisiones", "mactivos_margen", "mpasivos_margen", "ccajeros_propios_descuentos", "mcajeros_propios_descuentos", "ctarjeta_visa_descuentos", "mtarjeta_visa_descuentos", "ctarjeta_master_descuentos", "mtarjeta_master_descuentos", "ccomisiones_otras", "mcomisiones_otras", "chomebanking_transacciones") := NA]

# 202002 - ccajeros_propios_descuentos, mcajeros_propios_descuentos, ctarjeta_visa_descuentos
dataset[foto_mes == 202002, c("ccajeros_propios_descuentos", "mcajeros_propios_descuentos", "ctarjeta_visa_descuentos", "mtarjeta_visa_descuentos", "ctarjeta_master_descuentos", "mtarjeta_master_descuentos") := NA]

# 202009 - ccajeros_propios_descuentos, mcajeros_propios_descuentos, ctarjeta_visa_descuentos
dataset[foto_mes == 202009, c("ccajeros_propios_descuentos", "mcajeros_propios_descuentos", "ctarjeta_visa_descuentos", "mtarjeta_visa_descuentos", "ctarjeta_master_descuentos", "mtarjeta_master_descuentos") := NA]

# 202010 - ccajeros_propios_descuentos, mcajeros_propios_descuentos, ctarjeta_visa_descuentos
dataset[foto_mes == 202010, c("ccajeros_propios_descuentos", "mcajeros_propios_descuentos", "ctarjeta_visa_descuentos", "mtarjeta_visa_descuentos", "ctarjeta_master_descuentos", "mtarjeta_master_descuentos") := NA]

# 202102 - ccajeros_propios_descuentos, mcajeros_propios_descuentos, ctarjeta_visa_descuentos
dataset[foto_mes == 202102, c("ccajeros_propios_descuentos", "mcajeros_propios_descuentos", "ctarjeta_visa_descuentos", "mtarjeta_visa_descuentos", "ctarjeta_master_descuentos", "mtarjeta_master_descuentos") := NA]

# 202105 - ccajas_depositos
dataset[foto_mes == 202105, c("ccajas_depositos") := NA]


# Definicion de hiperparametros del modelo
PARAM <- list()
PARAM$experimento <- "E2010"
PARAM$semilla_primigenia <- 777787
PARAM$semilla_canarito <- 1021
PARAM$semilla_variables <- 42

# Semillas usadas
# 777781, 777787, 777817, 777839, 777857,100169, 100183, 100189, 100193, 100213, 103, 107, 109, 113, 127


PARAM$meses <- c(201901, 201902, 201903, 201904, 201905, 201906, 201907, 201908, 201909, 201910, 201911, 201912, 202001, 202002, 202003, 202004, 202005, 202007, 202008, 202009, 202010, 202011, 202012, 202101, 202102, 202103, 202104, 202105, 202106)

PARAM$undersampling <- 0.10

PARAM$future <- c(202108)

# -------------------------------------------------------------------
# Creacion de clase ternaria
# calculo el periodo0 consecutivo
dsimple <- dataset[, list(
  "pos" = .I,
  numero_de_cliente,
  periodo0 = as.integer(foto_mes/100)*12 +  foto_mes%%100 )
]
# ordeno
setorder( dsimple, numero_de_cliente, periodo0 )

# calculo topes
periodo_ultimo <- dsimple[, max(periodo0) ]
periodo_anteultimo <- periodo_ultimo - 1

# calculo los leads de orden 1 y 2
dsimple[, c("periodo1", "periodo2") :=
          shift(periodo0, n=1:2, fill=NA, type="lead"),  numero_de_cliente
]

# assign most common class values = "CONTINUA"
dsimple[ periodo0 < periodo_anteultimo, clase_ternaria := "CONTINUA" ]

# calculo BAJA+1
dsimple[ periodo0 < periodo_ultimo &
           ( is.na(periodo1) | periodo0 + 1 < periodo1 ),
         clase_ternaria := "BAJA+1"
]

# calculo BAJA+2
dsimple[ periodo0 < periodo_anteultimo & (periodo0+1 == periodo1 )
         & ( is.na(periodo2) | periodo0 + 2 < periodo2 ),
         clase_ternaria := "BAJA+2"
]

# pego el resultado en el dataset original y grabo
setorder( dsimple, pos )
dataset[, clase_ternaria := dsimple$clase_ternaria ]


# ------------------------------------------------------------------
# Ajuste por inflacion
# Datos de índice inflacionario
datos_inflacion <- data.table(
  foto_mes = c(
    201901, 201902, 201903, 201904, 201905, 201906, 201907, 201908, 201909, 201910, 201911, 201912,
    202001, 202002, 202003, 202004, 202005, 202006, 202007, 202008, 202009, 202010, 202011, 202012,
    202101, 202102, 202103, 202104, 202105, 202106, 202107, 202108
  ),
  indice = c(
    169.1809139, 159.4124222, 147.815783, 139.5635426, 132.4526539, 126.3017598, 121.434918,
    113.0124366, 101.1721247, 94.7578111, 86.80991363, 80.0686696, 76.10129252, 72.62525958,
    67.0403189, 64.57730013, 62.07688133, 58.52041377, 55.51249934, 51.42388727, 47.24862075,
    41.91137667, 37.56473688, 32.26670495, 27.11959515, 22.73236514, 17.09895961, 12.50829108,
    8.889903462, 5.539501357, 2.468486667, 0
  )
)

# Columnas seleccionadas para ajuste inflacionario
columnas_monetarias <- c(
  'mrentabilidad', 'mrentabilidad_annual', 'mcomisiones', 'mactivos_margen', 'mpasivos_margen',
  'mcuenta_corriente_adicional', 'mcuenta_corriente', 'mcaja_ahorro', 'mcaja_ahorro_adicional',
  'mcaja_ahorro_dolares', 'mcuentas_saldo', 'mautoservicio', 'mtarjeta_visa_consumo',
  'mtarjeta_master_consumo', 'mprestamos_prendarios',
  'mprestamos_hipotecarios', 'mplazo_fijo_dolares', 'mplazo_fijo_pesos', 'minversion1_pesos',
  'minversion1_dolares', 'minversion2', 'mpayroll', 'mpayroll2', 'mcuenta_debitos_automaticos',
  'mttarjeta_visa_debitos_automaticos', 'mttarjeta_master_debitos_automaticos', 'mpagodeservicios',
  'mpagomiscuentas', 'mcajeros_propios_descuentos', 'mtarjeta_visa_descuentos',
  'mtarjeta_master_descuentos', 'mcomisiones_mantenimiento', 'mcomisiones_otras',
  'mforex_buy', 'mforex_sell', 'mtransferencias_recibidas', 'mtransferencias_emitidas',
  'mextraccion_autoservicio', 'mcheques_depositados', 'mcheques_emitidos',
  'mcheques_depositados_rechazados', 'mcheques_emitidos_rechazados', 'matm', 'matm_other',
  'Master_mfinanciacion_limite', 'Master_msaldototal', 'Master_msaldopesos', 'Master_msaldodolares',
  'Master_mconsumospesos', 'Master_mconsumosdolares', 'Master_mlimitecompra',
  'Master_madelantopesos', 'Master_madelantodolares', 'Master_mpagado', 'Master_mpagospesos',
  'Master_mpagosdolares', 'Master_mconsumototal', 'Master_mpagominimo',
  'Visa_mfinanciacion_limite', 'Visa_msaldototal', 'Visa_msaldopesos', 'Visa_msaldodolares',
  'Visa_mconsumospesos', 'Visa_mconsumosdolares', 'Visa_mlimitecompra', 'Visa_madelantopesos',
  'Visa_madelantodolares', 'Visa_mpagado', 'Visa_mpagospesos', 'Visa_mpagosdolares',
  'Visa_mconsumototal', 'Visa_mpagominimo'
)

# Aplicar ajuste inflacionario
dataset <- merge(dataset, datos_inflacion, by = "foto_mes", all.x = TRUE)

for (col in columnas_monetarias) {
  dataset[, (col) := get(col) * (1 + indice / 100)]
}

cat("✅ Data actualizada correctamente a precios de julio 2021\n")
print(dataset[1:5, .SD, .SDcols = c("foto_mes", "mrentabilidad", "mrentabilidad_annual", "mcomisiones")])

# Eliminar columna indice
dataset[, indice := NULL]

# ------------------------------------------------------------------
# Hago undersampling de clase CONTINUA
undersample_banca <- function(dataset, seed = 42) {
  set.seed(seed)
  
  # Separar mes de aplicación (NO se toca)
  dataset_aplicacion <- dataset[foto_mes %in% c(202108)]
  
  # Train (meses anteriores) → Sí se undersamplea
  dataset_train <- dataset[foto_mes < 202108]
  
  # Porcentajes deseados por clase en TRAIN
  fracciones <- list(
    'CONTINUA' = 0.4,
    'BAJA+1' = 1.00,
    'BAJA+2' = 1.00
  )
  
  indices_finales <- c()
  
  for (clase in names(fracciones)) {
    idx <- dataset_train[clase_ternaria == clase, which = TRUE]
    n <- as.integer(length(idx) * fracciones[[clase]])
    idx_elegidos <- sample(idx, n, replace = FALSE)
    indices_finales <- c(indices_finales, idx_elegidos)
  }
  
  # Dataset reducido de entrenamiento
  dataset_train_reducido <- dataset_train[indices_finales]
  
  # Juntar TRAIN reducido + APLICACIÓN completo
  dataset_final <- rbindlist(list(dataset_train_reducido, dataset_aplicacion))
  
  return(dataset_final)
}

# Aplicar undersampling
dataset <- undersample_banca(dataset)

# ------------------------------------------------------------------
# Hago lags
# Ordenar dataset
setorder(dataset, numero_de_cliente, foto_mes)

# Columnas para LAG (excluyendo identificadores y target)
cols_to_lag <- setdiff(names(dataset), c("numero_de_cliente", "foto_mes", "clase_ternaria"))

# Función para crear LAGs y DELTAs
crear_lags <- function(dt, cols, n_lags = 2) {
  for (i in 1:n_lags) {
    # Crear LAGs
    lag_cols <- paste0(cols, "_lag", i)
    dt[, (lag_cols) := shift(.SD, i, type = "lag"), 
       by = numero_de_cliente, .SDcols = cols]
    
    # Crear DELTAs
    delta_cols <- paste0(cols, "_delta", i)
    for (j in seq_along(cols)) {
      dt[, (delta_cols[j]) := get(cols[j]) - get(lag_cols[j])]
    }
  }
  return(dt)
}

# Aplicar creación de LAGs
dataset <- crear_lags(dataset, cols_to_lag, n_lags = 2)


# ------------------------------------------------------------------
# Guardar dataset procesado
#fwrite(dataset, "base2.csv")

# ------------------------------------------------------------------
# Renombrar columnas
cat("=== NOMBRES ANTES ===\n", file = "cambios_nombres.txt")
cat(paste(names(dataset), collapse = "\n"), file = "cambios_nombres.txt", append = TRUE)


renombrar_columnas_codigo <- function(dataset, 
                                      mantener_columnas = c("numero_de_cliente", "clase_ternaria", "foto_mes")) {
  
  dt <- as.data.table(dataset)
  nombres_originales <- names(dt)
  
  # Identificar columnas a renombrar
  columnas_a_renombrar <- setdiff(nombres_originales, mantener_columnas)
  
  # Crear mapeo directo
  mapeo <- setNames(nombres_originales, nombres_originales)
  
  # Asignar nuevos nombres
  nuevos_nombres <- paste0("C0_", sprintf("%04d", seq_along(columnas_a_renombrar)))
  mapeo[columnas_a_renombrar] <- nuevos_nombres
  
  # Aplicar cambios
  setnames(dt, names(mapeo), mapeo)
  
  return(dt)
}

# Usar función
dataset <- renombrar_columnas_codigo(dataset)

cat("\n\n=== NOMBRES DESPUES ===\n", file = "cambios_nombres.txt", append = TRUE)
cat(paste(names(dataset), collapse = "\n"), file = "cambios_nombres.txt", append = TRUE)
# ------------------------------------------------------------------


# Agrego canaritos
# Canaritos
for(i in 1:10) {
  set.seed(PARAM$semilla_canarito + i)  # Semilla diferente para cada canarito
  dataset[, paste0("canarito", i) := runif(nrow(dataset))]
}




# -------------------------------------------------------------------
# Produccion
# -------------------------------------------------------------------


# training y future
Sys.time()

# se filtran los meses donde se entrena el modelo final
dataset_train_final <- dataset[foto_mes %in% PARAM$meses]

# Undersampling, van todos los "BAJA+1" y "BAJA+2" y solo algunos "CONTINIA"

set.seed(PARAM$semilla_primigenia, kind = "L'Ecuyer-CMRG")
dataset_train_final[, azar := runif(nrow(dataset_train_final))]
dataset_train_final[, training := 0L]

dataset_train_final[
  (azar <= PARAM$undersampling | clase_ternaria %in% c("BAJA+1","BAJA+2")),
  training := 1L
]

dataset_train_final[, azar:= NULL] # elimino la columna azar

# paso la clase a binaria que tome valores {0,1}  enteros
#  BAJA+1 y BAJA+2  son  1,   CONTINUA es 0
#  a partir de ahora ya NO puedo cortar  por prob(BAJA+2) > 1/40

dataset_train_final[,
                    clase01 := ifelse(clase_ternaria %in% c("BAJA+1","BAJA+2"), 1L, 0L)
]



# utilizo  zLightGBM  la nueva libreria
if( !require("zlightgbm") ) install.packages("https://storage.googleapis.com/open-courses/dmeyf2025-e4a2/zlightgbm_4.6.0.99.tar.gz", repos= NULL, type= "source")
require("zlightgbm")
Sys.time()


# canaritos
PARAM$qcanaritos <- 100

cols0 <- copy(colnames(dataset_train_final))
filas <- nrow(dataset_train_final)

for( i in seq(PARAM$qcanaritos) ){
  dataset_train_final[, paste0("canarito_",i) := runif( filas) ]
}

# las columnas canaritos mandatoriamente van al comienzo del dataset
cols_canaritos <- copy( setdiff( colnames(dataset_train_final), cols0 ) )
setcolorder( dataset_train_final, c( cols_canaritos, cols0 ) )

Sys.time()

# los campos que se van a utilizar

campos_buenos <- setdiff(
  colnames(dataset_train_final),
  c("clase_ternaria", "clase01", "training")
)

# dejo los datos en el formato que necesita LightGBM

dtrain_final <- lgb.Dataset(
  data= data.matrix(dataset_train_final[training == 1L, campos_buenos, with= FALSE]),
  label= dataset_train_final[training == 1L, clase01],
  free_raw_data= FALSE
)

cat("filas", nrow(dtrain_final), "columnas", ncol(dtrain_final), "\n")
Sys.time()

# definicion de parametros, los viejos y los nuevos

Sys.time()


PARAM$lgbm <-  list(
  boosting= "gbdt",
  objective= "binary",
  metric= "custom",
  first_metric_only= FALSE,
  boost_from_average= TRUE,
  feature_pre_filter= FALSE,
  force_row_wise= TRUE,
  lambda_l1= 2.5, 
  lambda_l2= 2.5,
  verbosity= -100,
  
  seed= PARAM$semilla_primigenia,
  
  max_bin= 31L,
  min_data_in_leaf= 20L,  #este ya es el valor default de LightGBM
  
  num_iterations= 9999L, # dejo libre la cantidad de arboles, zLightGBM se detiene solo
  num_leaves= 999L, # dejo libre la cantidad de hojas, zLightGBM sabe cuando no hacer un split
  learning_rate= 1.0,  # se lo deja en 1.0 para que si el score esta por debajo de gradient_bound no se lo escale
  
  feature_fraction= 0.50, # un valor equilibrado, habra que probar alternativas ...
  
  canaritos= PARAM$qcanaritos, # fundamental en zLightGBM, aqui esta el control del overfitting
  gradient_bound= 0.05  # default de zLightGBM
)


# entreno el modelo

modelo_final <- lgb.train(
  data= dtrain_final,
  param= PARAM$lgbm
)

Sys.time()

# grabo el modelo generado, esto pude ser levantado por LighGBM en cualquier maquina
lgb.save(modelo_final, file="zmodelo.txt")

# grabo un dataset que tiene el detalle de los arboles de LightGBM
tb_arboles <- lgb.model.dt.tree(modelo_final)
fwrite(tb_arboles, file="tb_arboles.txt", sep="\t")

cat("cantidad arbolitos=", tb_arboles[, max(tree_index)+1],"\n" )
cat("summary de las hojas de los arboles")
summary( tb_arboles[, list(hojas=max(leaf_index, na.rm=TRUE)+1), tree_index][,hojas])

Sys.time()

# aplico el modelo a los datos sin clase
dfuture <- dataset[foto_mes %in% PARAM$future]

# penosamente, en la versión actual de zLightGBM  los campos canaritos
#  aunque no se utilizan para nada, también deben estar en el dataset donde se hace el predict()
filas <- nrow(dfuture)

for( i in seq(PARAM$qcanaritos) ){
  dfuture[, paste0("canarito_",i) := runif( filas) ]
}

prediccion <- predict(
  modelo_final,
  data.matrix(dfuture[, campos_buenos, with= FALSE]),
)

# tabla de prediccion, puede ser util para futuros ensembles
#  ya que le modelo ganador va a ser un ensemble de LightGBMs

tb_prediccion <- dfuture[, list(numero_de_cliente, foto_mes)]
tb_prediccion[, prob := prediccion ]

# grabo las probabilidad del modelo
fwrite(tb_prediccion,
       file= "prediccion.txt",
       sep= "\t"
)


# 
# # genero archivos con los  "envios" mejores
# dir.create("kaggle", showWarnings=FALSE)
# 
# # ordeno por probabilidad descendente
# setorder(tb_prediccion, -prob)
# 
# envios <- 11000
# tb_prediccion[, Predicted := 0L] # seteo inicial a 0
# tb_prediccion[1:envios, Predicted := 1L] # marco los primeros
# 
# archivo_kaggle <- paste0("./kaggle/KA", PARAM$experimento, "_", envios, ".csv")
# 
# # grabo el archivo
# fwrite(tb_prediccion[, list(numero_de_cliente, Predicted)],
#        file= archivo_kaggle,
#        sep= ","
# )
# 
# 
# 
# 
# PARAM$semilla_kaggle <- 314159
# PARAM$cortes <- seq(500, 25000, by= 250)
# 
# 
# particionar <- function(data, division, agrupa= "", campo= "fold", start= 1, seed= NA) {
#   if (!is.na(seed)) set.seed(seed, "L'Ecuyer-CMRG")
#   
#   bloque <- unlist(mapply(
#     function(x, y) {rep(y, x)},division, seq(from= start, length.out= length(division))))
#   
#   data[, (campo) := sample(rep(bloque,ceiling(.N / length(bloque))))[1:.N],by= agrupa]
# }
# 
# 
# 
# 
# # iniciliazo el dataset de realidad, para medir ganancia
# realidad_inicializar <- function( pfuture, pparam) {
#   
#   # datos para verificar la ganancia
#   drealidad <- pfuture[, list(numero_de_cliente, foto_mes, clase_ternaria)]
#   
#   particionar(drealidad,
#               division= c(3, 7),
#               agrupa= "clase_ternaria",
#               seed= PARAM$semilla_kaggle
#   )
#   
#   return( drealidad )
# }
# 
# 
# # evaluo ganancia en los datos de la realidad
# realidad_evaluar <- function( prealidad, pprediccion) {
#   
#   prealidad[ pprediccion,
#              on= c("numero_de_cliente", "foto_mes"),
#              predicted:= i.Predicted
#   ]
#   
#   tbl <- prealidad[, list("qty"=.N), list(fold, predicted, clase_ternaria)]
#   
#   res <- list()
#   res$public  <- tbl[fold==1 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.3
#   res$private <- tbl[fold==2 & predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]/0.7
#   res$total <- tbl[predicted==1L, sum(qty*ifelse(clase_ternaria=="BAJA+2", 780000, -20000))]
#   
#   prealidad[, predicted:=NULL]
#   return( res )
# }
# 
# 
# 
# # inicilizo el dataset  drealidad
# drealidad <- realidad_inicializar( dfuture, PARAM)
# 
# # ordeno por probabilidad descendente
# setorder(tb_prediccion, -prob)
# 
# 
# 
# dir.create("kaggle2")
# 
# for (envios in PARAM$cortes) {
#   
#   tb_prediccion[, Predicted := 0L] # seteo inicial a 0
#   tb_prediccion[1:envios, Predicted := 1L] # marco los primeros
#   
#   archivo_kaggle <- paste0("./kaggle/KA", PARAM$experimento, "_", envios, ".csv")
#   
#   # grabo el archivo
#   fwrite(tb_prediccion[, list(numero_de_cliente, Predicted)],
#          file= archivo_kaggle,
#          sep= ","
#   )
#   
#   res <- realidad_evaluar( drealidad, tb_prediccion)
#   
#   options(scipen = 999)
#   cat( "Envios=", envios, "\t",
#        " TOTAL=", res$total,
#        "  Public=", res$public,
#        " Private=", res$private,
#        "\n",
#        sep= ""
#   )
#   
# }

