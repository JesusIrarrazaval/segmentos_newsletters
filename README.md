# Descripción
Este script automatiza la actualización de segmentos dinámicos en Amazon Pinpoint a partir de bases de datos alojadas en Redshift. Permite una administración eficiente de campañas de email marketing, asegurando la limpieza de correos y evitando envíos a contactos con rebotes o solicitudes de exclusión.

# Requisitos
- Python 3.9 o superior
- Acceso a Amazon Pinpoint y AWS IAM con permisos adecuados
- Acceso a una base de datos Redshift (o PostgreSQL compatible)
- `.env` con credenciales y configuraciones del entorno

# Instalación
- Creamos y activamos un entorno virtual (sólo la primera vez): `python -m venv venv`
- Activar el entorno virtual: `venv\Scripts\activate`
- Instalar dependencias: `pip install -r requirements.txt`
- Para desactivar el entorno virtual usamos `deactivate` en la terminal

# Instrucciones para trabajar con git

- Nos posicionamos en la rama principal o desde la rama que queremos comenzar a trabajar `git checkout master`
- Traemos los cambios `git pull origin master`
- Creamos una rama para trabajar en los cambios que vamos a proponer: `git checkout -b <nombre-de-la-rama>`
- Realizamos los cambios necesarios
- Actualizamos las dependencias `pip freeze > requirements.txt`
- Añadimos los cambios `git add .`
- Hacemos commit `git commit -m "YYYY-MM-DD Descripción de los cambios realizados"`
- Publicamos la rama en el repositorio en GitHub `git push -u origin <nombre-de-la-rama>`
- Crear una pull request en GitHub desde la rama nueva a la rama principal del repositorio.

# Estructura .env
## Redshift
host = xxxx
port = xxxx
database = xxxx
user = xxxx
password = xxxx

## AWS
accessKeyId = xxxx
secretAccessKey = xxxx
region = us-east-1
rolPinpoint = arn:aws:iam::XXXXXXXXXXXX:role/pinpointSegmentImport

# Checklist de tareas

Este listado permite garantizar la correcta ejecución del proceso, ya sea en entorno de pruebas o en producción:

### Preparación antes de realizar cambios

- [ ] Confirmar que estás en la rama correcta (debe ser `master`, no en `jesus`).
- [ ] Hacer un **pull** de la última versión del repositorio antes de realizar cambios.
- [ ] Cambiar a rama `jesus` para editar el proyecto.

### Verificación de configuración antes de pruebas

- [ ] Revisar que la variable `test` esté configurada correctamente según el entorno (debe estar en `True` para pruebas).
- [ ] Revisar que el archivo `.env` esté configurado correctamente.
- [ ] Verificar que la tabla `sistema.segmentos_test` esté actualizada respecto a la tabla original para evitar problemas de testeo ante la creación de nuevos newsletters.

### Revisión del código antes de la ejecución

- [ ] Asegurar que no se han cambiado las credenciales de los newsletters accidentalmente.
- [ ] Dejar activo solo el/los newsletters que se requieran probar para evitar modificar el resto innecesariamente.
- [ ] Verificar que la tabla `automatizaciones.registro_automatizaciones` **no** se vea modificada en fase de pruebas. 
- [ ] Revisar que no haya líneas de código comentadas innecesariamente que puedan afectar la ejecución (`Source Control`).
- [ ] Revisar que no se vean afectados los **newsletters que no han sido enviados** durante el día para no alterar las bases.

### Fase de pruebas

- [ ] Confirmar que los datos se están insertando correctamente en Redshift.
- [ ] Asegurar que los segmentos en Pinpoint se están actualizando correctamente.
- [ ] Verificar que no se modifique la tabla `automatizaciones.registro_automatizaciones`

### Ejecución en *modo producción*

- [ ] Verificar que la variable `test` esté en `False`.
- [ ] Verificar que estén activos los newsletters correspondientes en la variable `newsletters`.
- [ ] Verificar la correcta ejecución de todas las funciones.
- [ ] Verificar que la tabla `automatizaciones.registro_automatizaciones` se actualice correctamente.
- [ ] Revisar que los segmentos hayan sido debidamente actualizados en los newsletters.
- [ ] Revisar que la tabla `sistema.segmentos` se haya actualizado correctamente en Redshift.
- [ ] Revisar que se hayan borrado los archivos temporales en S3.


### Flujo de trabajo con Git

- [ ] Verificar los cambios realizados antes de hacer commit.
- [ ] Revisar que no haya líneas de código comentadas innecesariamente que puedan afectar la ejecución (`Source Control`).
- [ ] Asegurar que los archivos `.env` y otros datos sensibles están en `.gitignore`.
- [ ] Actualizar las dependencias `pip freeze > requirements.txt`
- [ ] Añadir los cambios `git add .`.
- [ ] Hacer commit `git commit -m "YYYY-MM-DD Descripción de los cambios realizados"`
- [ ] Publicar la rama en el repositorio en GitHub `git push -u origin <nombre-de-la-rama>` (recordar que si la rama ya está creada en GitHub, no debe llevar el `-u`).
- [ ] Crear una pull request en GitHub desde la rama nueva a la rama principal del repositorio.