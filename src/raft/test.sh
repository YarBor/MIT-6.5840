IsWrong=0
Times=0
while [ $IsWrong -eq 0 ]; do
    echo "Go test $Times Times"
    rm /home/wang/raftLog/* 2>/dev/null 1>/dev/null
    start_time=$(date +%s)
    go test > log
    IsWrong=$?
    elapsed_time1=$(( $(date +%s) - start_time ))
    echo "use : ${elapsed_time1}s"
    Times=$((Times+1))
done
echo "get WrongMsg"

