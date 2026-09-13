# Massa de dados sintética — Cooperativa NovaRota

Esta massa foi criada exclusivamente para o desafio técnico e não contém dados reais.

## Arquivos

- `clientes_cdc.csv`
- `contas_cdc.csv`
- `cartoes_cdc.csv`
- `eventos_risco.csv`
- `estornos.csv`
- `transacoes/transacoes_2026-01-31.csv`
- `transacoes/transacoes_2026-02-28.csv`
- `transacoes/transacoes_2026-03-31.csv`
- `transacoes/transacoes_2026-04-05_late.csv`
- `transacoes/transacoes_2026-04-10_schema_v2.csv`

## Cenários intencionais

### clientes_cdc
- múltiplas versões do mesmo cliente para histórico/SCD Tipo 2;
- registro duplicado;
- CPF sintético inválido;
- estado inválido;
- renda negativa.

### contas_cdc
- alterações de status para `ENCERRADA`;
- cliente inexistente para teste de integridade referencial;
- registro duplicado.

### cartoes_cdc
- alteração de limite;
- alteração de status para `CANCELADO`;
- versões históricas;
- cartão associado a conta inexistente.

### transacoes
- arquivos separados por data/lote;
- duplicidade dentro do mesmo lote;
- mesma `id_transacao` aparecendo em cargas diferentes;
- valores zero e negativo;
- cartão inexistente;
- lote com dados atrasados (`*_late.csv`);
- evolução de schema no arquivo `*_schema_v2.csv`, com `device_id` e `ip_origem`.

### eventos_risco
- eventos FRAUDE, CHARGEBACK e SUSPEITA;
- diferentes severidades;
- referência para transação inexistente;
- evento duplicado.

### estornos
- estornos em transações válidas;
- múltiplos estornos para uma mesma transação para simular cenário parcial/complementar;
- referência inválida.

## Observação importante sobre estornos

O layout mínimo do desafio não possui campo de valor do estorno. Portanto, os arquivos preservam exatamente
os campos mínimos solicitados: `id_estorno`, `id_transacao`, `data_estorno` e `motivo`.

Na implementação, deve ser documentada a premissa adotada para diferenciar estorno total/parcial.
Em produção, seria recomendado adicionar um campo como `valor_estorno` ou `percentual_estorno`.
