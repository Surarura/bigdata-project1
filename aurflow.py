from datetime import datetime
from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

# 'with DAG(...) as dag:' definiuje cały Twój "przepis" (pipeline).
# Wszystkie operatory (kroki) zdefiniowane w tym bloku będą należeć do tego DAGa.
with DAG(
    dag_id="projekt1_film_views", # Zmieniłem ID na unikalne dla Twojego projektu
    start_date=datetime(2025, 1, 1), # Data startowa, możesz ustawić dowolną
    schedule=None,
    params={
        # 'params' to zmienne, które możesz podać ręcznie przy uruchamianiu DAGa.
        # Twój prowadzący będzie ich używał do testowania [cite: 947-949].

        # {{ params.dags_home }} -> Ścieżka, gdzie leżą Twoje skrypty i JAR
        "dags_home": Param(
            "/home/TWOJ_LOGIN_NA_MASZYNIE_MASTER/airflow/dags", # <-- UZUPEŁNIJ TO NA GCP
            type="string"
        ),
        # {{ params.input_dir }} -> Ścieżka do GŁÓWNEGO folderu z danymi
        "input_dir": Param(
            "gs://TWOJ_BUCKET/input", # <-- UZUPEŁNIJ TO NA GCP
            type="string"
        ),
        # {{ params.output_mr_dir }} -> Folder na wynik z MapReduce
        "output_mr_dir": Param("/project1/output_mr_film_views", type="string"),
        # {{ params.output_dir }} -> Folder na finalny wynik z Hive
        "output_dir": Param("/project1/output_film_views", type="string"),

        # Ten parametr decyduje, czy uruchomić 'mapreduce_classic' czy 'hadoop_streaming'
        # Ustawiamy 'classic' jako domyślne dla Ciebie[cite: 642, 643].
        "classic_or_streaming": Param(
            "classic", enum=["classic", "streaming"]
        ),
    },
    render_template_as_native_obj=True,
    catchup=False,
) as dag:

    # Krok 1: Usuwanie starych katalogów wyjściowych[cite: 670].
    # 'BashOperator' to po prostu krok, który wykonuje komendę w terminalu[cite: 874].
    # '{{ params.output_mr_dir }}' to magiczna składnia Airflow (nazywa się Jinja),
    # która wstrzykuje wartość parametru 'output_mr_dir' zdefiniowanego wyżej.
    clean_output_mr_dir = BashOperator(
        task_id="clean_output_mr_dir",
        bash_command=(
            # Sprawdź, czy katalog istnieje, jeśli tak, usuń go siłowo i rekursywnie.
            # '|| true' jest dodane, aby komenda nie zwróciła błędu, jeśli katalogu nie ma.
            "if hadoop fs -test -d {{ params.output_mr_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_mr_dir }}; fi"
        ),
    )

    clean_output_dir = BashOperator(
        task_id="clean_output_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_dir }}; fi"
        ),
    )

    # Krok 2: Rozgałęzienie (Branch)
    # Ten krok decyduje, którą ścieżkę wybrać na podstawie parametru [cite: 698-699].
    def _pick_classic_or_streaming(params):
        if params["classic_or_streaming"] == "classic":
            return "mapreduce_classic" # Zwraca ID zadania, które ma być uruchomione
        else:
            return "hadoop_streaming"

    pick_classic_or_streaming = BranchPythonOperator(
        task_id="pick_classic_or_streaming",
        python_callable=_pick_classic_or_streaming,
        op_kwargs={"params": dag.params},
    )

    # Krok 3a: Twoje zadanie MapReduce Classic (UZUPEŁNIONE) [cite: 721-722]
    # To jest polecenie, które Airflow wykona, jeśli parametr 'classic_or_streaming' to 'classic'.
    mapreduce_classic = BashOperator(
        task_id="mapreduce_classic",
        bash_command=(
            "hadoop jar {{ params.dags_home }}/project_files/FilmViewsMapReduce-1.0-SNAPSHOT.jar " # 1. Twój JAR
            "org.example.bigdata.FilmStats " # 2. Twoja klasa główna
            "{{ params.input_dir }}/datasource1 " # 3. Ścieżka do danych MR (input)
            "{{ params.output_mr_dir }}" # 4. Ścieżka wyjściowa MR
        ),
    )

    # Krok 3b: Puste zadanie dla streamingu (nie robisz go)
    hadoop_streaming = BashOperator(
        task_id="hadoop_streaming",
        bash_command="echo 'Pominięto - wybrano wariant classic'",
    )

    # Krok 4: Uruchomienie skryptu Hive (UZUPEŁNIONE) [cite: 726-727]
    hive = BashOperator(
        task_id="hive",
        bash_command=(
            # Używamy beeline, tak jak w Twoim skrypcie run_hive.sh
            "beeline -u jdbc:hive2://localhost:10000/default "
            # Wskazujemy plik HQL, który leży w folderze dags/project_files
            "-f {{ params.dags_home }}/project_files/hive_script.hql "
            # Przekazujemy zmienne do skryptu HQL
            "--hiveconf input_dir3={{ params.output_mr_dir }} "
            "--hiveconf input_dir4={{ params.input_dir }}/datasource4 "
            "--hiveconf output_dir6={{ params.output_dir }}"
        ),
        # Ta zasada mówi: "Uruchom mnie, jeśli poprzedni krok się udał LUB został pominięty"
        # Jest to KLUCZOWE, aby Hive uruchomił się niezależnie, czy poszliśmy ścieżką classic czy streaming.
        trigger_rule="none_failed",
    )

    # Krok 5: Pobranie i wyświetlenie końcowego wyniku [cite: 744, 1145]
    get_output = BashOperator(
        task_id="get_output",
        bash_command=(
            # -getmerge łączy wszystkie pliki wynikowe (np. part-r-00000...) z katalogu Hive
            # w jeden plik JSON w lokalnym systemie plików maszyny.
            # '&& head ...' natychmiast wyświetla pierwsze 10 linii tego pliku w logach Airflow.
            "hadoop fs -getmerge {{ params.output_dir }} output6.json && head output6.json"
        ),
        trigger_rule="none_failed",
    )

    # Krok 6: Definicja zależności (KOLEJNOŚĆ WYKONYWANIA) [cite: 763-770]
    # [lista] >> zadanie : Oznacza, że zadanie uruchomi się po ukończeniu WSZYSTKICH zadań z listy.
    [clean_output_mr_dir, clean_output_dir] >> pick_classic_or_streaming

    # pick_classic_or_streaming uruchomi ALBO mapreduce_classic ALBO hadoop_streaming
    pick_classic_or_streaming >> [mapreduce_classic, hadoop_streaming]

    # hive poczeka, aż zadanie classic lub streaming się zakończy
    [mapreduce_classic, hadoop_streaming] >> hive

    # get_output uruchomi się po hivie
    hive >> get_output