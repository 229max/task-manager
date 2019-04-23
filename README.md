# task-manager
manager task runner with nameko

how to use


1. start master

nameko run task_master


2. start slave more if you want

nameko run task_slave --config nameko_config.yaml


3. start client to start tasks
python3 client.py
