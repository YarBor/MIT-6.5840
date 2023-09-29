IsWrong=0
while [ $IsWrong -eq 0 ]; do
    rm /home/wang/raftLog/* 2>/dev/null 1>/dev/null 
    go test -run 2C
    IsWrong=$?
done