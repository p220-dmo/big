setxkbmap fr

sudo yum –y install openssh-server openssh-clients


##sur CentOS
commande pour installer serveur ssh
sudo yum install  openssh-server  
systemctl enable sshd    
systemctl start sshd    
systemctl status sshd  
firewall-cmd  --zone=public --add-port=22/tcp --permanent   
firewall-cmd --reload 
yum install -y ntp  
##Putty
ulimit -Sn    ulimit -n 64000   
systemctl disable firewalld    
systemctl status firewalld    
systemctl stop firewalld 

wget  -nv  http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.4.0/ambari.repo  -O /etc/yum.repos.d/ambari.repo


Wget http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.4.0/ambari.repo -O /etc/yun.repos.d/ambari.repo

[root@osboxes examples]# sudo lsof -i -P -n | grep 53

dnsmasq    1690    nobody    5u  IPv4   24695      0t0  UDP 192.168.122.1:53
dnsmasq    1690    nobody    6u  IPv4   24696      0t0  TCP 192.168.122.1:53 (LISTEN)
