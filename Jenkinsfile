pipeline {
  agent { label 'ubuntu' }
  stages {
    stage('pre') {
      steps {
        echo 'start'
      }
    }
    stage('build') {
      parallel {
        stage('JDK 8') {
          tools {
	    jdk 'JDK 1.8 (latest)'
	  }
	  steps {
            sh 'gradle -version'
          }
        }

        stage('JDK 11') {
          tools {
	    jdk 'JDK 11 (latest)'
	  }
	  steps {
            sh 'gradle -version'
            githubNotify context: 'JDK 11 build', description: 'Does this work?',  status: 'SUCCESS'
          }
        }
      }
    }
    stage('post') {
      steps {
        echo 'finish'
      }
    }
  }
}
