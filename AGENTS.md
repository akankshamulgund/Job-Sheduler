# Project rules

- Use Java 21 and Spring Boot 3 for the backend; use Maven.
- Keep code simple, readable, and commented where intent is not obvious.
- Write tests for hard logic, especially scheduling, retries, claiming, and failure handling.
- Never commit `.env` files, credentials, tokens, or other secrets.
- Run backend tests with `cd backend && ./mvnw test` (Windows: `mvnw.cmd test`, once the wrapper is added) or `mvn test` when Maven is installed.
- Run the full integration suite with Docker available; Testcontainers starts its own test dependencies.
- Run the UI checks from `ui/` with `npm install` and `npm test` once the UI is populated.

Keep design and implementation aligned with `docs/design.md`.
