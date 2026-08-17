# Mantenimiento del repositorio — actualizado 2026-08-17

## Publicación y automatización

- Origen canónico privado: `https://github.com/LynxPardelle/zoolanding-commerce`.
- Ramas base publicadas: `main`, `test` y `dev`; promoción `dev -> test -> main`.
- CI valida cada push y pull request con permisos de lectura. Los Environments
  restringen `test` a la rama `test` y `production` a `main`.
- Roles OIDC/CloudFormation y topic de alarmas están configurados sin claves AWS
  estáticas. Cada Environment tiene una clave de cursor aleatoria y distinta;
  GitHub sólo expone su metadata, no su valor.
- Validación local: 283/283 pruebas, compilación, SAM, Actionlint y Gitleaks.

## Despliegue pendiente

**NO-GO para desplegar la aplicación.** Sólo existen las identidades retenidas.
Faltan el stack Commerce, parámetros SSM canónicos y servicios Integrations
publicados; el topic de alarmas tiene cero suscriptores confirmados. Los gates
fiscales, de pago y de aceptación siguen cerrados.

La protección de ramas privadas no está incluida en el plan GitHub actual. No
se hizo público el código para eludir esa restricción. Use pull requests, CI y
pushes normales; nunca fuerce historia.

No transfiera `.env`, claves de firma, datos de pago/fiscales, PII, payloads de
proveedor, `.aws-sam`, cachés ni entornos virtuales. Clone el código desde GitHub.
