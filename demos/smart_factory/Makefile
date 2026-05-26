.PHONY: build-frontend deploy dev clean

build-frontend:
	cd frontend && npm ci && npm run build

deploy: build-frontend
	databricks bundle deploy -t dev

deploy-prod: build-frontend
	databricks bundle deploy -t prod

dev:
	cd frontend && npm run dev &
	uvicorn app:app --reload --port 8000

clean:
	rm -rf frontend/dist frontend/node_modules
