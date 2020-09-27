#!/usr/bin/env bash

COMMAND=${1}

check_input_files() {
  files=(movies_metadata.csv enwiki-latest-abstract.xml)

  for input_file in "${files[@]}"; do
      if [[ -e "./input/${input_file}" ]]; then
        echo "Checking input file: ${input_file}... OK"
      else
        echo "Checking input file: ${input_file}... File does not exist. Exiting"
        exit 1
      fi
  done

}

docker_compose_up() {
  echo "Starting Docker Stack if not already running..."
  docker-compose up -d
  echo -e "Starting Docker Stack if not already running... OK"
}

docker_compose_down() {
  echo "Stopping Docker Stack if running..."
  docker-compose down
  echo -e "Stopping Docker Stack if running... OK"
}

docker_compose_logs() {
  echo "Showing Docker Stack logs when running:"
  docker-compose logs -f -t
}

process_data() {
  docker exec -it -w /app spark-master bash -c '/spark/bin/spark-submit --conf spark.jars.packages="${PYSPARK_PACKAGES}" --master local[*] process_data.py'
}

test_code() {
    docker exec -it -w /app spark-master bash -c 'python3 -m unittest discover'
}

case "${COMMAND}" in
start)
  docker_compose_up
  ;;
stop)
  docker_compose_down
  ;;
logs)
  docker_compose_logs
  ;;
process_data)
  check_input_files
  process_data
  ;;
test)
  test_code
  ;;
*)
  echo "Usage: truefilm-cli.sh (start|stop|logs|process_data|test)"
  exit 1
  ;;
esac
