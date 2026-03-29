pipeline {
    agent any

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timeout(time: 30, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    environment {
        COMPOSE_FILE = 'docker-compose.yml'

        // ── Postgres credentials ──────────────────────────────────────────────
        DB_HOST     = 'db'
        DB_PORT     = '5432'
        DB_NAME     = credentials('etl-postgres-db')
        DB_USER     = credentials('etl-postgres-user')
        DB_PASSWORD = credentials('etl-postgres-password')
    }

    stages {

        // ─────────────────────────────────────────────────────────────────────
        stage('Checkout') {
            steps {
                echo "Checking out ${env.GIT_BRANCH}..."
                checkout scm
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        stage('Environment Check') {
            steps {
                sh '''
                    docker --version
                    docker compose version
                    python3 --version || true
                '''
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Ensure the ETL postgres is healthy before running the pipeline.
        // ─────────────────────────────────────────────────────────────────────
        stage('Ensure Database') {
            steps {
                sh '''
                    DB_STATUS=$(docker inspect retail_db --format="{{.State.Status}}" 2>/dev/null || echo "missing")

                    if [ "$DB_STATUS" = "running" ]; then
                        echo "✓ Database is already running"
                    else
                        echo "Database not running — starting via ETL compose..."
                        docker compose -p etl -f ${COMPOSE_FILE} up -d db

                        echo "Waiting for database to be healthy..."
                        for i in $(seq 1 12); do
                            DB_STATUS=$(docker inspect retail_db --format="{{.State.Status}}" 2>/dev/null || echo "missing")
                            if [ "$DB_STATUS" = "running" ]; then
                                echo "✓ Database is running"
                                break
                            fi
                            echo "Attempt ${i}/12 — DB status: ${DB_STATUS}, waiting..."
                            sleep 5
                        done

                        DB_STATUS=$(docker inspect retail_db --format="{{.State.Status}}" 2>/dev/null || echo "missing")
                        if [ "$DB_STATUS" != "running" ]; then
                            echo "✗ Database failed to reach running state"
                            exit 1
                        fi
                    fi
                '''
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        stage('Build ETL Image') {
            steps {
                echo "Building ETL image..."
                sh 'docker build -t retail_analytics:latest .'
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        stage('Run ETL') {
            steps {
                echo "Running ETL pipeline via docker compose..."
                sh '''
                    export DB_HOST="${DB_HOST}"
                    export DB_PORT="${DB_PORT}"
                    export DB_USER="${DB_USER}"
                    export DB_PASSWORD="${DB_PASSWORD}"
                    export DB_NAME="${DB_NAME}"

                    # Remove any stale container from a previous run
                    docker rm -f retail_analytics 2>/dev/null || true

                    docker compose -p etl -f ${COMPOSE_FILE} up --no-build --remove-orphans
                '''
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        stage('Verify Output') {
            steps {
                sh '''
                    echo "Checking ETL container exit code..."
                    EXIT_CODE=$(docker inspect retail_analytics --format="{{.State.ExitCode}}" 2>/dev/null || echo "1")
                    if [ "$EXIT_CODE" = "0" ]; then
                        echo "✓ ETL completed successfully (exit code 0)"
                    else
                        echo "✗ ETL failed with exit code: ${EXIT_CODE}"
                        docker logs retail_analytics --tail=50
                        exit 1
                    fi
                '''
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        stage('Cleanup') {
            steps {
                sh '''
                    docker compose -p etl -f ${COMPOSE_FILE} down --remove-orphans || true
                    docker image prune -f || true
                '''
            }
        }

    }

    post {
        success {
            echo "✓ ETL pipeline completed successfully on branch ${env.GIT_BRANCH}"
        }
        failure {
            echo "✗ ETL pipeline failed — check logs above"
            sh '''
                docker compose -p etl -f docker-compose.yml logs --tail=50 || true
            '''
        }
        always {
            echo "Build #${env.BUILD_NUMBER} on branch ${env.GIT_BRANCH} — done."
        }
    }
}
