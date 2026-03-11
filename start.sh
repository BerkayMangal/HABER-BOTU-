#!/bin/bash
# Her botu paralel calistir
python main.py &
python fa_bot.py &
python viop_server.py &
wait -n
exit $?
