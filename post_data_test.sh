#!/bin/bash

# Define the list of Python scripts
scripts=("fm.py" "netconf_o1_cu.py" "ru_pm.py" "socket_info.py" "ue_info.py")

# Loop through and execute each Python script in a new tmux session
for i in {0..4}
do
  # Create a new tmux session and execute the Python script using python3
  tmux new-session -d -s "python_session_$((i+1))" "python3 ${scripts[$i]}"
  
  # Wait for the tmux session to start (you can adjust the sleep duration if needed)
  sleep 10
done

echo "5 Python sessions have been started"
