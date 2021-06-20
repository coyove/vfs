#!/bin/sh
END=$1
if [ "$END" = "" ]; then
    END=100
fi
echo test $END times

go test -c  -o test_exec

for i in $(seq 1 $END); do
    echo '#' $i
    ./test_exec | grep -i fail

    if [ $(python3 -c 'import random; print(random.randint(0, 2))') = "0" ]; then
        echo 'delete'
        rm test.*
    fi
done

rm test_exec
