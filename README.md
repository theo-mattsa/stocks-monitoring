## Requisitos
* Docker
* Docker Compose

## Subindo o cluster
No diretório onde está o `docker-compose.yaml`, execute:

```bash
docker compose up -d
```

Verifique se os 3 containers estão no ar:

```bash
docker compose ps
```

## Parar o ambiente
```bash
docker compose down
```

Para remover também os volumes (dados):
```bash
docker compose down -v
```