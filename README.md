# Automação Studio Velocity

Este repositório contém dois scripts principais:

- `automation.py`: coleta e filtra aulas disponíveis de acordo com as regras de
  negócio especificadas.
- `telegram_notification.py`: utiliza o resultado do módulo `automation` para
  enviar um resumo das aulas disponíveis para um chat do Telegram.

## Execução local

1. Crie e ative um ambiente virtual, depois instale as dependências:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Exporte as variáveis de ambiente necessárias (quando quiser enviar a
   notificação de fato):

   ```bash
   export TELEGRAM_BOT_TOKEN="seu-token"
   export TELEGRAM_CHAT_ID="seu-chat-id"
   ```

3. Execute a automação isoladamente para inspecionar o JSON retornado:

   ```bash
   python automation.py
   ```

4. Para enviar o resumo pelo Telegram utilize:

   ```bash
   python telegram_notification.py
   ```

   Caso deseje apenas visualizar a mensagem formatada sem enviá-la (por exemplo,
   em um teste local), acrescente a opção `--dry-run`:

   ```bash
   python telegram_notification.py --dry-run
   ```

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

## Execução automática no GitHub Actions

Este repositório inclui o workflow
`.github/workflows/studio-automation.yml`, responsável por executar a
automação automaticamente todos os dias, a cada 1 hora, das 10h às 23h
(horário de São Paulo). O workflow também pode ser disparado manualmente
pela interface do GitHub (opção **Run workflow**).

Durante a execução, o workflow irá:

1. Fazer o checkout do repositório.
2. Configurar o Python 3.11.
3. Instalar as dependências listadas em `requirements.txt`.
4. Executar `python telegram_notification.py`, reutilizando os segredos
   `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID` configurados anteriormente.

## Monitoramento do tempo de execução

O módulo `automation` agora registra o horário de início, término e o
tempo total gasto em cada execução. Essas informações são exibidas no
terminal sempre que `telegram_notification.py` é executado, inclusive no
GitHub Actions. Quando o workflow roda na plataforma do GitHub, o tempo
de execução também é anexado automaticamente ao **Step Summary** da
execução, facilitando a consulta posterior.
