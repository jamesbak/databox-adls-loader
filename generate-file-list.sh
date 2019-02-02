#!/usr/bin/env bash

source_path=$1
unit_size=$2

readarray -t dirs < <(hadoop fs -du -x $source_path)
readarray -t sizes < <(IFS='';for line in ${dirs[@]}; do echo $line | cut -d ' ' -f 1; done)
readarray -t dir_names < <(IFS='';for line in ${dirs[@]}; do echo '/'$(echo $line | cut -d '/' -f 2-); done)

dir_unit_number=()
unit_space_available=()
for ((i=0; i < ${#sizes[@]}; i++)); do
    echo "Processing dir: $i ${dir_names[$i]} ${sizes[$i]}"
    allocated=0
    for ((j=0; j < ${#unit_space_available[@]}; j++)); do
        echo "$j: ${unit_space_available[$j]}"
        if [[ ${unit_space_available[$j]} -ge ${sizes[$i]} ]]; then
            dir_unit_number[$i]=$(($j + 1))
            unit_space_available[$j]=$((${unit_space_available[$j]} - ${sizes[$i]}))
            allocated=1
            echo "Allocated in existing unit"
            break
        fi
    done
    if [[ $allocated == 0 ]]; then
        j=${#unit_space_available[@]}
        unit_space_available+=($unit_size)
        dir_unit_number[$i]=$(($j + 1))
        unit_space_available[$j]=$((${unit_space_available[$j]} - ${sizes[$i]}))
        echo "Allocated in new unit"
    fi
done