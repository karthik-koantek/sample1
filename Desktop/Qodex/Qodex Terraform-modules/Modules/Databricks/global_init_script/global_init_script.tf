############global init script
resource "databricks_global_init_script" "init2" {
  content_base64 = base64encode(<<-EOT
    %sh
    #!/bin/bash
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
    apt-get update
    ACCEPT_EULA=Y apt-get install msodbcsql17
    apt-get -y install unixodbc-dev
    sudo apt-get install python3-pip -y
    pip3 install --upgrade pyodbc    
    EOT
  )
  name = "pyodbc install"
  enabled = true
}

