# Automação Studio Velocity

Este repositório contém dois scripts principais:

- `automation.py`: coleta e filtra aulas disponíveis de acordo com as regras de
  negócio especificadas.
- `telegram_notification.py`: utiliza o resultado do módulo `automation` para
  enviar um resumo das aulas disponíveis para um chat do Telegram.

## Configuração do Token do Telegram como segredo no GitHub

Para manter o token do bot em sigilo, configure-o como um **segredo de
repositório** nas configurações do GitHub:

1. Acesse o repositório no GitHub e clique em **Settings**.
2. No menu lateral, escolha **Secrets and variables → Actions**.
3. Clique em **New repository secret**.
4. Defina o nome do segredo como `TELEGRAM_BOT_TOKEN` e cole o token fornecido
   pelo BotFather no campo **Secret**.
5. Repita o processo para armazenar o `TELEGRAM_CHAT_ID`, caso queira evitar que
   o identificador do chat apareça em claro.

Os scripts esperam as variáveis de ambiente `TELEGRAM_BOT_TOKEN` e
`TELEGRAM_CHAT_ID` durante a execução (por exemplo, em um workflow do
GitHub Actions).
