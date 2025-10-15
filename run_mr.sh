#!/bin/bash

# Sprawdzenie, czy podano dokładnie dwa argumenty
if [ "$#" -ne 2 ]; then
    echo "Użycie: $0 <input_dir_hdfs> <output_dir_hdfs>"
    exit 1
fi

# Przypisanie argumentów do zmiennych dla czytelności
INPUT_DIR=$1
OUTPUT_DIR=$2

# Pobranie nazwy pliku JAR z folderu target (zakładamy, że jest tam tylko jeden)
JAR_FILE=$(find target -name "*.jar")

# Pobranie nazwy klasy sterującej z kodu (można też wpisać na sztywno)
MAIN_CLASS="org.example.bigdata.FilmStats"

echo "================================================="
echo "Uruchamianie zadania MapReduce..."
echo "Plik JAR: $JAR_FILE"
echo "Katalog wejściowy: $INPUT_DIR"
echo "Katalog wyjściowy: $OUTPUT_DIR"
echo "================================================="

# Usuwanie katalogu wyjściowego w HDFS, jeśli istnieje.
# To zapewnia, że zadanie zawsze może wystartować bez błędu. [cite: 75]
hdfs dfs -rm -r -f $OUTPUT_DIR

# Uruchomienie zadania MapReduce
hadoop jar $JAR_FILE $MAIN_CLASS $INPUT_DIR $OUTPUT_DIR

# Sprawdzenie kodu wyjścia ostatniej komendy
if [ $? -eq 0 ]; then
    echo "Zadanie MapReduce zakończone pomyślnie."
    echo "Aby zobaczyć wynik, użyj komendy: hdfs dfs -cat $OUTPUT_DIR/part-r-00000"
else
    echo "Wystąpił błąd podczas wykonywania zadania MapReduce."
fi