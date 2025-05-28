import psycopg2
import pandas as pd

def subir_consolidado_a_postgres(csv_path, db_params):
    # Leer el CSV consolidado
    df = pd.read_csv(csv_path)

    # Conectar a PostgreSQL
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO evaluacion_videos (
                id_estudiante, nombre, curso, video_url,
                duracion_segundos, fecha_envio,
                transcripcion_limpia, claridad,
                argumentacion, originalidad, promedio_final
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_estudiante) DO NOTHING
        """, (
            row['id_estudiante'],
            row['nombre'],
            row['curso'],
            row['video_url'],
            int(row['duracion_segundos']),
            row['fecha_envio'],
            row['transcripcion_limpia'],
            float(row['claridad']),
            float(row['argumentacion']),
            float(row['originalidad']),
            float(row['promedio_final'])
        ))

    conn.commit()
    cur.close()
    conn.close()
