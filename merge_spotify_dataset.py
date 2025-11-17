#!/usr/bin/env python3
"""
Merge de todos los CSV de spotify_dataset/ en un único dataset
y generación de estadísticas básicas para preparar los datos de ML.

- Lee todos los .csv de spotify_dataset/
- Añade columna 'session_file' con el nombre del fichero de origen
- Se asegura de que exista la columna 'PC' (si no, la crea asumiendo PC=1)
- Guarda:
    - spotify_dataset/spotify_merged_dataset.csv
    - spotify_dataset/spotify_merged_summary.txt
"""

import os
import glob
import pandas as pd

DATA_DIR = "spotify_dataset"
OUTPUT_CSV = "spotify_merged_dataset.csv"
OUTPUT_SUMMARY = "spotify_merged_summary.txt"


def load_and_merge(data_dir: str) -> pd.DataFrame:
    pattern = os.path.join(data_dir, "*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No se han encontrado CSV en {data_dir}")

    dfs = []
    print("Ficheros encontrados:")
    for f in files:
        print(f"  - {os.path.basename(f)}")
        try:
            df = pd.read_csv(f)

            # Añadimos columna con el nombre del fichero (identificador de sesión)
            df["session_file"] = os.path.basename(f)

            # Si no existía la columna PC (capturas antiguas), asumimos PC=1 (porque vienen de la VM)
            if "PC" not in df.columns:
                df["PC"] = 1

            dfs.append(df)
        except Exception as e:
            print(f"[!] Error leyendo {f}: {e}")

    if not dfs:
        raise RuntimeError("No se ha podido cargar ningún CSV correctamente")

    merged = pd.concat(dfs, ignore_index=True)
    return merged


def compute_summary(df: pd.DataFrame, out_path: str):
    with open(out_path, "w") as f:
        f.write("=== RESUMEN GLOBAL DATASET SPOTIFY ===\n\n")

        # Forma general
        f.write(f"Total filas: {len(df)}\n")
        f.write(f"Total columnas: {len(df.columns)}\n\n")

        # Columnas disponibles
        f.write("Columnas presentes:\n")
        for col in df.columns:
            f.write(f"  - {col}\n")
        f.write("\n")

        # Distribución por calidad
        if "quality_setting" in df.columns:
            f.write("=== Distribución por calidad (quality_setting) ===\n")
            f.write(df["quality_setting"].value_counts(dropna=False).to_string())
            f.write("\n\n")

        # Distribución por PC/Mobile
        if "PC" in df.columns:
            f.write("=== Distribución por dispositivo (PC=1, Mobile=0) ===\n")
            f.write(df["PC"].value_counts(dropna=False).to_string())
            f.write("\n\n")

        # Sesiones (ficheros)
        if "session_file" in df.columns:
            f.write("=== Número de filas por fichero de captura (session_file) ===\n")
            counts_by_file = df["session_file"].value_counts()
            f.write(counts_by_file.to_string())
            f.write("\n\n")

        # Valores nulos
        f.write("=== Valores nulos por columna ===\n")
        nulls = df.isna().sum()
        f.write(nulls.to_string())
        f.write("\n\n")

        # Describe de columnas numéricas
        f.write("=== Estadísticas numéricas globales ===\n")
        numeric_desc = df.describe(include="number")
        f.write(numeric_desc.to_string())
        f.write("\n\n")

        # Throughput por calidad (si existe)
        if "throughput_kbps" in df.columns and "quality_setting" in df.columns:
            f.write("=== Throughput_kbps por calidad (describe) ===\n")
            thr_by_quality = df.groupby("quality_setting")["throughput_kbps"].describe()
            f.write(thr_by_quality.to_string())
            f.write("\n\n")

        # IAT por calidad (si existe)
        if "iat" in df.columns and "quality_setting" in df.columns:
            f.write("=== IAT (Inter-Arrival Time) por calidad (describe) ===\n")
            iat_by_quality = df.groupby("quality_setting")["iat"].describe()
            f.write(iat_by_quality.to_string())
            f.write("\n\n")

        # Top destinos (para ver si hay “ruido” de muchas IPs distintas)
        if "dst_ip" in df.columns:
            f.write("=== Top 10 IPs destino (dst_ip) por número de paquetes ===\n")
            dst_counts = df["dst_ip"].value_counts().head(10)
            f.write(dst_counts.to_string())
            f.write("\n\n")

        if "src_ip" in df.columns:
            f.write("=== Top 10 IPs origen (src_ip) por número de paquetes ===\n")
            src_counts = df["src_ip"].value_counts().head(10)
            f.write(src_counts.to_string())
            f.write("\n\n")

        f.write("=== Fin del resumen ===\n")

    print(f"✓ Resumen guardado en: {out_path}")


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, DATA_DIR)

    print(f"Directorio de datos: {data_dir}")
    merged = load_and_merge(data_dir)

    # Guardar dataset unificado
    out_csv_path = os.path.join(data_dir, OUTPUT_CSV)
    merged.to_csv(out_csv_path, index=False)
    print(f"✓ Dataset unificado guardado en: {out_csv_path}")
    print(f"  Total filas: {len(merged)}")

    # Generar resumen
    out_summary_path = os.path.join(data_dir, OUTPUT_SUMMARY)
    compute_summary(merged, out_summary_path)


if __name__ == "__main__":
    main()
