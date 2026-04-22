./scripts/stop-services.sh
sleep 1
./scripts/start-services.sh 2 2
sleep 1
./scripts/check-services.sh