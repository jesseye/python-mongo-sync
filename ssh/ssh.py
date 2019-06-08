import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh.connect(hostname='192.168.137.101', port=22, username='hugo', password='1234567890')
stdin, stdout, stderr = ssh.exec_command('ls -lrt')

result = stdout.read()

print(result.decode())
ssh.close()

# pip3 freeze > req.text
# pip3 download -d .\packages -r req.text

# pip install --no-index --find-index=.\packages -r req.txt

# pip install --download d:\python27\packs pandas（-r requirements.txt）

# pip install --no-index --find-links=d:\python27\packs\ pandas （-r requirements.txt）
