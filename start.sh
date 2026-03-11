#!/bin/bash
# Her iki botu paralel calistir
python main.py &
python fa_bot.py &
wait -n
exit $?
