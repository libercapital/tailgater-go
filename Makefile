start-infra:
	docker-compose --project-directory ./e2e -p tailgater-infra up $(arg)

stop-infra:
	docker-compose --project-directory ./e2e -p tailgater-infra stop 
