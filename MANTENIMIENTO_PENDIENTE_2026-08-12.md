# Mantenimiento pendiente — 2026-08-12

## Publicación bloqueada de forma segura

Este repositorio no tiene ningún remoto configurado. El código y sus ramas se
conservaron localmente; no se inventó un destino, propietario o visibilidad.

- Destino candidato, sujeto a aprobación: `LynxPardelle/zoolanding-commerce`.
- Visibilidad recomendada hasta revisar arquitectura y rollout: **privada**.
- Rama actual: `codex/phase8-infrastructure-readiness`.
- Validación local: 283/283 pruebas, compilación Python y
  `sam validate --lint` correctos.
- Despliegue: **NO-GO**; el propio contrato del servicio declara esta fase
  local y exige gates posteriores. No se llamó AWS ni un proveedor.

Cuando exista un repositorio aprobado, configure `origin` únicamente con la URL
confirmada y publique primero esta rama de trabajo con un push normal. Después
revise las ramas históricas y promueva mediante `dev -> test -> main`; no fuerce
historia ni publique directamente una rama protegida.

Antes de transferir este repositorio, excluya `.env`, credenciales, payloads de
proveedor, datos fiscales/PII, resultados de SAM y entornos virtuales. Ninguno de
esos materiales debe entrar en Git ni en el paquete de código compartido.
