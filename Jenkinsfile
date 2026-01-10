pipeline {
  agent any

  environment {
    IMAGE_NAME         = 'beencolabs/degenter-api'
    DOCKER_CREDENTIALS = 'dockerhub-credentials'
    GIT_CREDENTIALS    = 'github-token'
    DEPLOY_USER        = 'root'
    DEPLOY_HOST        = '159.223.28.88'
    DEPLOY_PATH        = '/opt/degenter-api'
    GIT_URL            = 'https://github.com/cryptocomicsdevs/degenter-api.git'
    GIT_BRANCH         = 'masterr'
    SSH_KEY_ID         = 'server-ssh-key'
  }

  stages {
    stage('Checkout') {
      steps {
        git branch: env.GIT_BRANCH,
            credentialsId: env.GIT_CREDENTIALS,
            url: env.GIT_URL
      }
    }

    stage('Install & Build') {
      agent { docker { image 'node:18-bullseye' } }
      environment {
        NPM_CONFIG_CACHE = "${env.WORKSPACE}/.npm-cache"
      }
      steps {
        sh 'mkdir -p "$NPM_CONFIG_CACHE"'
        sh 'npm install'
        sh 'npm test || true'
      }
    }

    stage('Build Docker Image') {
      steps {
        sh "docker build -t ${IMAGE_NAME}:${BUILD_NUMBER} ."
      }
    }

    stage('Push to DockerHub') {
      steps {
        withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS,
                                          usernameVariable: 'DOCKER_USER',
                                          passwordVariable: 'DOCKER_PASS')]) {
          sh """
            echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
            docker push ${IMAGE_NAME}:${BUILD_NUMBER}
            docker tag  ${IMAGE_NAME}:${BUILD_NUMBER} ${IMAGE_NAME}:latest
            docker push ${IMAGE_NAME}:latest
            docker logout
          """
        }
      }
    }

    stage('Deploy to Server') {
      steps {
        sshagent(credentials: [SSH_KEY_ID]) {
          sh """
            ssh -o StrictHostKeyChecking=no ${DEPLOY_USER}@${DEPLOY_HOST} '
              set -e
              docker pull ${IMAGE_NAME}:latest
              cd ${DEPLOY_PATH}
              docker compose up -d
            '
          """
        }
      }
    }
  }

  post {
    always {
      sh 'docker image prune -f || true'
    }
  }
}