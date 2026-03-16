import os
# Importamos la paleta de colores de reportlab para dar estilo al texto y fondos
from reportlab.lib import colors
# Importamos el tamaño de hoja estándar A4
from reportlab.lib.pagesizes import A4
# Importamos las herramientas para manejar estilos de texto y documentos base
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
# Importamos las unidades de medida (pulgadas) para configurar márgenes y espacios
from reportlab.lib.units import inch
# Importamos las clases principales de Platypus, el motor de maquetación de ReportLab:
# SimpleDocTemplate: La plantilla de documento más básica.
# Paragraph: Un bloque de texto que responde a estilos de párrafo.
# Spacer: Un elemento vacío utilizado para agregar espacio vertical u horizontal.
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
# Importamos KeepTogether para evitar que un bloque (ej. un paso y su código) 
# quede dividido entre dos páginas al imprimir
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="guia_conexion_github.pdf"):
    """
    Función principal para generar el documento PDF.
    
    Esta función utiliza ReportLab y su sistema Platypus para construir
    un documento a partir de una lista de elementos (Flowables) que se
    van apilando en orden: encabezado, introducción, pasos, código y pie de página.
    """
    
    # 1. Configurar la plantilla del documento
    # SimpleDocTemplate se encarga de crear el lienzo base del PDF.
    # Le pasamos el nombre del archivo de salida, el tamaño A4 y definimos 
    # un margen de 1 pulgada (2.54 cm) en todos los bordes para darle un aspecto limpio.
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # 2. Definición del sistema de estilos
    # getSampleStyleSheet() nos devuelve un conjunto de estilos predefinidos (Normal, Title, Heading1, etc.)
    # Usaremos estos como clase "padre" para crear nuestros propios estilos personalizados.
    styles = getSampleStyleSheet()
    
    # Estilo personalizado para el título principal del documento
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],           # Hereda propiedades base de Title
        fontName='Helvetica-Bold',        # Forzamos tipografía en negrita
        fontSize=24,                      # Tamaño de fuente grande (24 puntos)
        leading=28,                       # Interlineado (espacio desde la línea base hasta la siguiente)
        textColor=colors.HexColor("#2A4365"), # Color hexadecimal: Azul oscuro profesional
        spaceAfter=20,                    # Espaciado extra de 20 puntos debajo del título
        alignment=1                       # Alineación: 1 = Centrado (0=Izquierda, 2=Derecha, 4=Justificado)
    )

    # Estilo personalizado para el párrafo introductorio
    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,                       # Interlineado de 18 puntos (1.5x el tamaño de fuente, buena legibilidad)
        textColor=colors.HexColor("#4A5568"), # Color hexadecimal: Gris medio para relajar la vista
        spaceAfter=20
    )

    # Estilo personalizado para los subtítulos correspondientes a cada paso de la guía
    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"), # Azul intermedio vibrante para resaltar
        spaceBefore=15,                   # Asegura espacio *antes* del título del paso
        spaceAfter=8
    )

    # Estilo para los párrafos de texto explicativo dentro de cada paso
    body_style = ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        leading=16,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=8
    )

    # Estilo muy personalizado para simular bloques de código de terminal
    code_style = ParagraphStyle(
        name='CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',               # Fuente monoespaciada para el código
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#E2E8F0"), # Texto gris claro / casi blanco
        backColor=colors.HexColor("#1A202C"), # Fondo gris muy oscuro (temática Dark Mode)
        borderPadding=(8, 10, 8, 10),     # Relleno interior (Arriba, Derecha, Abajo, Izquierda)
        borderWidth=1,                    # Ancho del borde
        borderColor=colors.HexColor("#2D3748"), # Color del borde sutil
        borderRadius=5,                   # Bordes redondeados del recuadro
        spaceBefore=5,
        spaceAfter=15
    )

    # 3. Ensamblaje de la "Historia" (Story)
    # Story es una lista simple de Python. Platypus irá tomando cada bloque de 
    # esta lista y lo imprimirá en el PDF de arriba hacia abajo.
    story = []

    # 3.1 Añadimos el encabezado estilo "Supra-título"
    # Fíjate que al Paragraph le podemos definir el estilo "al vuelo" si deseamos.
    story.append(Paragraph("Guía Profesional de Python", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    
    # 3.2 Añadimos el título principal utilizando nuestro estilo 'title_style'
    story.append(Paragraph("Conexión del Proyecto a GitHub", title_style))
    
    # Añadimos una separación visual: Spacer (ancho, alto) - en este caso 0.2 pulgadas de alto
    story.append(Spacer(1, 0.2 * inch))

    # 3.3 Introducción narrativa
    # Podemos utilizar etiquetas HTML simples (como <b> para negritas) dentro del texto
    intro_text = (
        "El control de versiones es esencial en cualquier proyecto de software moderno. "
        "Esta guía detalla los pasos para inicializar un repositorio local con <b>Git</b> y "
        "vincularlo a un repositorio remoto en <b>GitHub</b> usando la herramienta <b>GitHub CLI (gh)</b>."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Definición de la estructura de datos: Los pasos de la guía
    # Mantenemos el contenido desvinculado del diseño iterando sobre una lista de diccionarios.
    steps_data = [
        {
            "title": "1. Instalar Herramientas Necesarias",
            "body": "Primero, asegúrate de tener instalados tanto Git como GitHub CLI. Puedes descargarlos de forma manual o usar el gestor de paquetes de Windows (winget):",
            "code": "winget install --id Git.Git -e --source winget\nwinget install --id GitHub.cli -e --source winget"
        },
        {
            "title": "2. Crear el archivo .gitignore",
            # Nota cómo usamos el tag <font> de ReportLab para simular código *inline* dentro del párrafo normal
            "body": "Antes de empezar con Git, es fundamental crear un archivo <font name='Courier' color='#D53F8C'>.gitignore</font> en la raíz de tu proyecto para evitar que se suban archivos innecesarios como el directorio <font name='Courier' color='#D53F8C'>venv/</font> o la caché de Python.",
            "code": "echo venv/ >> .gitignore\necho __pycache__/ >> .gitignore"
        },
        {
            "title": "3. Inicializar el Repositorio Local",
            # También tenemos soporte para escapar comillas literales con \" dentro del diccionario 
            "body": "Navega a la carpeta de tu proyecto y ejecuta los siguientes comandos para crear un repositorio Git, agregar todos los archivos (excepto los ignorados) y crear el commit inicial.",
            "code": "git init\ngit add .\ngit commit -m \"Initial commit\""
        },
        {
            "title": "4. Autenticarse en GitHub CLI",
            "body": "Para poder crear el repositorio en tu cuenta desde la terminal, necesitas iniciar sesión. Sigue las instrucciones del comando y verifica la sesión a través de tu navegador.",
            "code": "gh auth login"
        },
        {
            "title": "5. Crear y Subir el Repositorio Remote",
            "body": "Usaremos GitHub CLI para crear el repositorio en la nube, configurarlo como la ubicación remota (<font name='Courier' color='#D53F8C'>origin</font>) y subir tu código en un solo paso.",
            "code": "gh repo create nombre-de-tu-proyecto --source=. --remote=origin --push --public"
        }
    ]

    # Iterar a través de los datos de cada paso para convertirlos a bloques imprimibles
    for step in steps_data:
        # Por cada paso, creamos una pequeña sublista temporal (step_flowables).
        # Esto agrupa el Título, el Texto principal y el Bloque de Código de ese paso específico.
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        # Como Paragraph procesa texto como HTML y nosotros escribimos \n para los saltos de línea,
        # limpiamos el formato convirtiendo los saltos de línea nativos en tags <br/>.
        code_text = step["code"].replace('\n', '<br/>')
        
        # Añadimos el bloque de código ya formateado a la sublista
        step_flowables.append(Paragraph(code_text, code_style))
        
        # La magia de KeepTogether: Al envolver step_flowables en KeepTogether y 
        # pasarlo a nuestra story general, Platypus calculará si el grupo cabe en 
        # la pantalla actual. Si este bloque se 'rompiera' enviando la mitad a 
        # otra carilla, Platypus moverá el bloque ENTERO a la hoja siguiente.
        story.append(KeepTogether(step_flowables))
        
        # Un espacio adicional debajo de cada bloque de pasos completo
        story.append(Spacer(1, 10))

    # 5. Pie de página
    # Un "Spacer" largo al final empuja este bloque hacia el final del documento visible.
    # En proyectos grandes esto se maneja con "PageTemplates", pero aquí bastará al final del flujo.
    story.append(Spacer(1, 30))
    
    footer_style = ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',     # Letra inclinada
        fontSize=9,                       # Letra pequeña
        textColor=colors.HexColor("#A0AEC0"),
        alignment=1                       # Centrado
    )
    story.append(Paragraph("Generado de forma automatizada. Guía de GitHub CLI.", footer_style))

    # 6. Renderizar y Guardar PDF
    # Finalmente le indicamos al motor que tome nuestra plantilla 'doc' (con sus márgenes)
    # y dibuje todos los componentes apilados en 'story'.
    doc.build(story)
    
    # Informamos al usuario la ruta absoluta final por consola
    print(f"PDF generado exitosamente en: {os.path.abspath(filename)}")

# Comprobación típica de módulos de Python: 
# Si ejecutamos este script directamente (python script.py), ejecutará la función.
# Si lo importamos desde otro archivo (`import generar_pdf`), no ejecutará nada de forma invisible.
if __name__ == "__main__":
    create_pdf()
