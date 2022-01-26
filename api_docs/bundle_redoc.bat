call py -3 ./redocly_config_to_json.py
call redoc-cli bundle openapi.yaml --options .redocly.json --cdn=true --output=redoc-static.html
call py -3 ./update_html_with_custom_css.py

