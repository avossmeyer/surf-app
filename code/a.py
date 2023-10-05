print(5)
import os
os.system("echo Hello from the other side!")
with open ('/tmp/rand.txt','a') as f:
    f.write(' {} - Your random number is n'.format(5))