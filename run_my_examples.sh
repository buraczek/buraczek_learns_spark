#!/usr/bin/env bash
#set -x

ENV_IMGE_NAME=buraczek-spark-env

function process_file(){
  local LC="\033[0;36m"
  local NC="\033[0m"

  for file in ${1}; do
    echo -e "${LC}Processed file: ${file}${NC}"

    local file_name=$(basename "${file}")
    local extension="${file_name##*.}"
    local language=""

    if [[ ${extension} == "sc" ]]; then
       language="scala"

       docker run -v "$(pwd)/buraczek_code_examples:/buraczek_code_examples" \
       -a stdin -a stdout -it -p 4040:4040 ${ENV_IMGE_NAME}:v1 \
       spark-shell -I /buraczek_code_examples/${language}/${file_name} 2>&1 | grep -vE '^[[:digit:]]{2}\/[[:digit:]]{2}\/[[:digit:]]{2}'
    fi

    if [[ ${extension} == "py" ]]; then
      language="python"

      docker run -v "$(pwd)/buraczek_code_examples:/buraczek_code_examples" \
      -a stdin -a stdout -it -p 4040:4040 ${ENV_IMGE_NAME}:v1 \
      spark-submit 2>&1 /buraczek_code_examples/${language}/${file_name} | grep -vE '^[[:digit:]]{2}\/[[:digit:]]{2}\/[[:digit:]]{2}'

    fi
  done
}

# re-create docker env if needed
docker build -t ${ENV_IMGE_NAME}:v1 - < Dockerfile

# open firefox just in case
#firefox > /dev/null 2>&1  -new-tab http://127.0.0.1:4040 &

# if no file was used as an argument, all files will be processed
if [[ -z "$1" ]]; then

    files_to_process=$(find $(pwd)/buraczek_code_examples/python/chapter*.py -type f)
    process_file "${files_to_process}"

    files_to_process=$(find $(pwd)/buraczek_code_examples/scala/chapter*.sc -type f)
    process_file "${files_to_process}"

else

    files_to_process=${1}
    process_file "${files_to_process}"

fi
