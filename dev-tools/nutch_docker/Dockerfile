# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM centos:centos6

MAINTAINER Talat UYARER

# Install Git, which is missing from the Ubuntu base images.
RUN yum update && apt-get install -y git

# Add the dependencies from the hbase_docker folder and delete ones we don't need.
WORKDIR /root

# Install Java.
ENV JDK_URL http://download.oracle.com/otn-pub/java/jdk/7u67-b01/jdk-7u67-linux-x64.tar.gz
RUN mkdir -p /usr/java
RUN wget -nv --header "Cookie: oraclelicense=accept-securebackup-cookie" -O - $JDK_URL | tar xzf - --strip-components=1 -C "/usr/java"
ENV JAVA_HOME /usr/java

# Install Maven.
ENV MAVEN_URL http://apache.claz.org/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz
RUN mkdir -p /usr/local/apache-maven
RUN wget -nv -O - $MAVEN_URL | tar xzf - --strip-components=1 -C "/usr/local/apache-maven"
ENV MAVEN_HOME /usr/local/apache-maven

# Install Ant
ENV ANT_URL http://archive.apache.org/dist/ant/binaries/apache-ant-1.7.1-bin.tar.gz
RUN mkdir -p /usr/local/apache-ant
RUN wget -nv -O - $ANT_URL | tar xzf - --strip-components=1 -C "/usr/local/apache-ant"
ENV ANT_HOME /usr/local/apache-ant

# Add Java and Maven to the path.
ENV PATH $JAVA_HOME/bin:$MAVEN_URL/bin:$ANT_URL/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Pull down HBase and build it into /root/hbase-bin.
RUN git clone http://git.apache.org/hbase.git -b master
RUN mvn clean install -DskipTests assembly:single -f ./hbase/pom.xml
RUN mkdir -p hbase-bin
RUN tar xzf /root/hbase/hbase-assembly/target/*tar.gz --strip-components 1 -C /root/hbase-bin

# Set HBASE_HOME, add it to the path, and start HBase.
ENV HBASE_HOME /root/hbase-bin
ENV PATH $HBASE_HOME/bin:/usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN ["/bin/bash", "-c", "start-hbase.sh"]

# Pull down Nutch and build it into /root/nutch.
RUN git clone http://git.apache.org/nutch.git -b 2.x
RUN ant -f nutch/build.xml
