#Elastic Serach 

 sudo apt-get install elasticsearch
  sudo apt-get update
  sudo apt-get install openjdk-7-jre-headless -y
  ELASTICSEARCH_VERSION=1.5.2
  wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-${ELASTICSEARCH_VERSION}.deb
  sudo dpkg -i elasticsearch-1.5.2.deb
  curl -L http://github.com/elasticsearch/elasticsearch-servicewrapper/tarball/master | tar -xz
  mkdir /usr/local/share/elasticsearch
  mkdir /usr/local/share/elasticsearch/bin
  mv *servicewrapper*/service /usr/local/share/elasticsearch/bin/
  ls
  rm -Rf *servicewrapper*
  /usr/local/share/elasticsearch/bin/service/elasticsearch install
  sudo ln -s `readlink -f /usr/local/share/elasticsearch/bin/service/elasticsearch` /usr/local/bin/rcelasticsearch
  service elasticsearch start
  curl http://localhost:9200


#Python dependencies
apt-get install python-pip

