# Executing this command through a shell script for two reasons:
# 1. Using the nice command allows CPU priority to stay where it's needed to
#    decode transmissions.
# 2. The trunk-recorder application doesn't like calling a python script.

nice -n 19 python3 post-call-script.py $1
