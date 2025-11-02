#!/bin/bash

# Sprawdzenie, czy podano 3 argumenty
if [ "$#" -ne 3 ]; then
    echo "Użycie: $0 <input_dir_mr> <input_dir_films> <output_dir_hive>"
    exit 1
fi

# Argumenty (ścieżki w HDFS/GCS)
INPUT_MR=$1
INPUT_FILMS=$2
OUTPUT_HIVE=$3

echo "================================================="
echo "Uruchamianie skryptu Hive..."
echo "Input (MR): $INPUT_MR"
echo "Input (Filmy): $INPUT_FILMS"
echo "Output (Hive): $OUTPUT_HIVE"
echo "================================================="

# Uruchomienie skryptu HQL za pomocą beeline
# -f : plik ze skryptem
# --hiveconf : przekazuje zmienne do skryptu HQL
beeline -u "jdbc:hive2://" -f hive_script.hql \
    --hiveconf input_dir3=$INPUT_MR \
    --hiveconf input_dir4=$INPUT_FILMS \
    --hiveconf output_dir6=$OUTPUT_HIVE

# Sprawdzenie kodu wyjścia
if [ $? -eq 0 ]; then
    echo "Skrypt Hive zakończony pomyślnie."
else
    echo "Wystąpił błąd podczas wykonywania skryptu Hive."
fi