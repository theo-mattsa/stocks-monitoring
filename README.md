# Stocks Monitoring 📈

Trata-se de um sistema orientado a eventos que utiliza dados da API da BRAPI para monitorar informações relacionadas a ações da bolsa de valores.

## Requisitos
* Java 17
* Docker
* Docker Compose
* Possuir uma chave de API da `brapi.dev`

## Configurando a chave de API
Crie um arquivo `.env` na raiz do projeto e adicione seu token da BRAPI:
```bash
BRAPI_TOKEN=seu_token_aqui
```

## Subindo o cluster
No diretório onde está o `docker-compose.yaml`, execute:

```bash
sudo docker compose up -d
```

Verifique se os 3 containers estão no ar:

```bash
sudo docker compose ps
```

## Parar o ambiente
```bash
sudo docker compose down
```

Para remover também os volumes (dados):
```bash
sudo docker compose down -v
```

## Troubleshooting

Para subir o cluster nos computadores do LabGrad sem sudo, execute o seguinte comando: 

```bash
dockerd-rootless-setuptool.sh install
```
## Descrição dos mocks
- `vale.json` - Dados simulados da Vale apresentando uma **inversão de tendência**, com alta inicial, formação de pico e posterior movimento de baixa.
- `itau.json` - Dados simulados do Itaú contendo uma **variação significativa de preço** entre dois períodos.
- `bradesco.json` - Dados simulados do Bradesco com um **pico anormal de volume de negociações**, permitindo testar alertas relacionados a volume fora do padrão.
- `magalu.json` - Dados simulados do Magazine Luiza apresentando uma **tendência de baixa**, ou seja, redução gradual dos preços ao longo do tempo.
- `weg.json` - Dados simulados da WEG apresentando uma **tendência de alta**, ou seja, aumento gradual dos preços ao longo do tempo.