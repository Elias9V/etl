import pandas as pd
from extraccion.obtener_datos_video import obtener_datos_video
from transformacion.obtener_evaluacion_transcripcion import obtener_evaluacion_transcripcion
from carga.subir_a_postgres import subir_consolidado_a_postgres
from config.db_params import db_params

def consolidar_y_guardar():
    # 1. Extracción
    video = obtener_datos_video()
    evaluacion = obtener_evaluacion_transcripcion()

    # 2. Transformación
    df_video = pd.DataFrame([video])
    df_eval = pd.DataFrame([evaluacion])

    # 3. Consolidado (JOIN)
    df_final = pd.merge(df_video, df_eval, on="id_estudiante")

    # 4. Guardar como CSV
    output_path = "etl_videos_ia/datos/salida/dataset_consolidado.csv"
    df_final.to_csv(output_path, index=False)
    print(f"✅ Consolidado guardado como CSV en: {output_path}")

    # 5. Cargar en PostgreSQL
    subir_consolidado_a_postgres(output_path, db_params)
    print("✅ Consolidado cargado en PostgreSQL.")

if __name__ == "__main__":
    consolidar_y_guardar()
