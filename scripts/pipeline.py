# pipeline de transacciones
# lee el csv de gcs, valida y escribe en bigquery: raw, stg y errores

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

proyecto = "prueba-covalto-data"
entrada = "gs://covalto-transacciones-fabian/2026-08-13/transacciones.csv"
temp = "gs://covalto-temp-fabian/temp"

columnas = ["id_transaccion", "id_cliente", "monto", "tipo", "fecha_transaccion", "moneda"]
esquema_raw = ",".join(f"{c}:STRING" for c in columnas)
esquema_stg = "id_transaccion:STRING,id_cliente:STRING,monto:FLOAT,tipo:STRING,fecha_transaccion:TIMESTAMP,moneda:STRING"
esquema_err = esquema_raw + ",motivo_error:STRING"


def parsear(linea):
    # arma un diccionario a partir de la linea del csv
    return dict(zip(columnas, linea.split(",")))


def revisar(fila):
    # devuelve el motivo del error, o none si esta bien
    if not fila["id_cliente"]:
        return "cliente_vacio"
    try:
        if float(fila["monto"]) <= 0:
            return "monto_invalido"
    except ValueError:
        return "monto_no_numerico"
    return None


def tipar(fila):
    # convierte los campos al tipo que espera bigquery
    return {**fila, "monto": float(fila["monto"]), "moneda": fila["moneda"].strip().upper()}


def mas_reciente(grupo):
    # de las filas con el mismo id, se queda con la mas reciente
    _, filas = grupo
    return max(filas, key=lambda f: f["fecha_transaccion"])


def escribir(tabla, esquema):
    return beam.io.WriteToBigQuery(
        f"{proyecto}:{tabla}",
        schema=esquema,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )


def run():
    options = PipelineOptions(
        runner="DataflowRunner", #DirectRunner para correr local, DataflowRunner para correr en la nube
        project=proyecto,
        region="us-central1",
        temp_location=temp,
        job_name="transacciones",
    )

    with beam.Pipeline(options=options) as p:

        filas = (
            p
            | "leer" >> beam.io.ReadFromText(entrada, skip_header_lines=1)
            | "parsear" >> beam.Map(parsear)
        )

        # raw: el dato como llego
        filas | "guardar raw" >> escribir("raw.transacciones", esquema_raw)

        # quita duplicados, se queda con la transaccion mas reciente
        unicas = (
            filas
            | "clave por id" >> beam.Map(lambda f: (f["id_transaccion"], f))
            | "agrupar" >> beam.GroupByKey()
            | "quitar duplicados" >> beam.Map(mas_reciente)
        )

        # stg: las filas que pasan la validacion, ya tipadas
        (
            unicas
            | "filtrar buenas" >> beam.Filter(lambda f: revisar(f) is None)
            | "tipar" >> beam.Map(tipar)
            | "guardar stg" >> escribir("stg.transacciones", esquema_stg)
        )

        # errores: las que no pasan, con el motivo
        (
            unicas
            | "filtrar malas" >> beam.Filter(lambda f: revisar(f) is not None)
            | "marcar" >> beam.Map(lambda f: {**f, "motivo_error": revisar(f)})
            | "guardar errores" >> escribir("stg.errores", esquema_err)
        )

    print("pipeline ok")


if __name__ == "__main__":
    run()