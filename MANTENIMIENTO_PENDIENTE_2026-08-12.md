# Mantenimiento del repositorio — actualizado 2026-08-17

## Publicación y automatización

- Origen canónico público: `https://github.com/LynxPardelle/zoolanding-commerce`.
- Ramas base publicadas: `main`, `test` y `dev`; promoción `dev -> test -> main`.
- CI valida cada push y pull request con permisos de lectura. Los Environments
  restringen `test` a la rama `test` y `production` a `main`.
- Roles OIDC/CloudFormation, topic de alarmas y clave de cursor se consumen sólo
  como secretos del Environment y sólo en los pasos que los necesitan. No hay
  claves AWS estáticas ni valores de configuración operativa en el repositorio.
- Las antiguas variables duplicadas de `test` y `production` se retiraron sólo
  después de que el commit que consume los secretos completó CI en GitHub.
- `dev`, `test` y `main` exigen PR y CI estricto, incluyen a administradores,
  resuelven conversaciones y bloquean force-push y borrado. Secret scanning,
  push protection, patrones no-proveedor y validación de credenciales están activos.
- Validación local: 287/287 pruebas (tres pases), compilación, `pip-audit`, SAM
  lint/build, verificación de 11 funciones empaquetadas, Actionlint y Gitleaks.
- La defensa local limita inventario rastreado a 10 unidades por línea y 20 por
  checkout, reduce el throttle público a 2 rps/burst 4, habilita reconciliación
  programada y autentica rutas protegidas antes de resolver políticas.

## Despliegue pendiente

**NO-GO para desplegar la aplicación.** Sólo existen las identidades retenidas.
Faltan el stack Commerce, parámetros SSM canónicos y servicios Integrations
publicados; el topic de alarmas tiene cero suscriptores confirmados. Los gates
fiscales, de pago y de aceptación siguen cerrados.

La defensa local reduce, pero no elimina, el bloqueo distribuido de inventario:
un actor puede rotar claves de recuperación y origen de red. Antes de activar
checkout con inventario rastreado se requiere admisión externa breve, presupuesto
atómico por actor/identidad y controles edge por IP con evidencia de pruebas.

La protección de ramas no reemplaza el aprobador independiente todavía pendiente
en los Environments. Use pull requests y no fuerce historia.

No transfiera `.env`, claves de firma, datos de pago/fiscales, PII, payloads de
proveedor, `.aws-sam`, cachés ni entornos virtuales. Clone el código desde GitHub.
