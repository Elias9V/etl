from extraccion.obtener_datos_video import obtener_datos_video
from transformacion.obtener_evaluacion_transcripcion import obtener_evaluacion_transcripcion
import pandas as pd
from carga.subir_a_postgres import subir_consolidado_a_postgres
from config.db_params import db_params
import os

def ejecutar_pipeline():
    # 📥 Extraer datos
    datos_video = obtener_datos_video()
    datos_eval = obtener_evaluacion_transcripcion()

    # 📊 Convertir a DataFrame
    df_video = pd.DataFrame(datos_video)
    df_eval = pd.DataFrame(datos_eval)

    # 🔗 Unir por ID
    df_final = pd.merge(df_video, df_eval, on="id_estudiante")

    # 💾 Guardar CSV consolidado
    output_dir = "etl_videos_ia/datos/salida"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "dataset_consolidado.csv")
    df_final.to_csv(output_path, index=False)
    print(f"✅ Dataset consolidado guardado en: {output_path}")

    # 🛢️ Subir a PostgreSQL
    subir_consolidado_a_postgres(output_path, db_params)
    print("✅ Datos cargados correctamente en la tabla 'evaluacion_videos'.")

if __name__ == "__main__":
    ejecutar_pipeline()
