$PackagePath = "./python/lib/python3.13/site-packages"

New-Item -ItemType Directory -Path $PackagePath -Force

pip install `
    --platform manylinux2014_x86_64 `
    --target=$PackagePath `
    --implementation cp `
    --python-version 313 `
    --only-binary=:all: `
    --upgrade `
    xlrd openpyxl

Compress-Archive -Path "python" -DestinationPath "read_excel_layer.zip" -Force 