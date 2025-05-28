CREATE TABLE evaluacion_videos (
    id_estudiante VARCHAR PRIMARY KEY,
    nombre VARCHAR NOT NULL,
    curso VARCHAR NOT NULL,
    video_url TEXT,
    duracion_segundos INT,
    fecha_envio DATE,
    transcripcion_limpia TEXT,
    claridad INT CHECK (claridad BETWEEN 0 AND 20),
    argumentacion INT CHECK (argumentacion BETWEEN 0 AND 20),
    originalidad INT CHECK (originalidad BETWEEN 0 AND 20),
    promedio_final INT CHECK (promedio_final BETWEEN 0 AND 20)
);
