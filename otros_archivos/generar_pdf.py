import os
# Importamos la paleta de colores nativa de reportlab para dar color a fuentes, fondos y bordes
from reportlab.lib import colors
# Importamos el formato estándar A4. Reportlab dispone de LETTER, LEGAL, etc.
from reportlab.lib.pagesizes import A4
# Obtenemos la hoja de estilos de muestra y la clase para aplicar CSS-like properties al documento
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
# Utilidad temporal para conversiones: 1 inch = 72 puntos, la medida base de PDF.
from reportlab.lib.units import inch
# Plataforma principal 'Platypus' (Page Layout and Typography Using Scripts):
# - SimpleDocTemplate: Proporciona la logística para ubicar los elementos secuencialmente en las hojas.
# - Paragraph: Elemento textual capaz de asimilar saltos de línea y formateo de etiquetas tipo XML.
# - Spacer: El equivalente visual al <br> en HTML; crea separaciones exactas controlando ancho y alto.
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
# Componente crucial: KeepTogether empaqueta múltiples elementos visuales. Mide si la suma de las 
# alturas de estos entrará en la hoja actual, o si debe empujarlos juntos a la siguiente página.
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="guia_entorno_virtual.pdf"):
    """
    Función que ensambla de forma metódica un archivo PDF.
    
    Genera un informe técnico paso a paso utilizando de forma abstracta
    una lista (story) de contenidos que irán construyendo un flujo vertical. 
    """
    # 1. Configurar el lienzo base: Definición de Template y Espaciado global
    # Instanciamos la plantilla que guardará todas las páginas.
    # Determinamos los límites donde puede existir texto usando márgenes.
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # 2. Hojas de Estilos en Cascada (El equivalente a CSS en PDF)
    # Genera copias instanciadas de los metadatos prediseñados de la librería (títulos, listas interactivas)
    styles = getSampleStyleSheet()
    
    # --- Diseñamos la Jerarquía Visual de Textos ---
    
    # Estilo del Título Principal
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],           # Utilizar las configuraciones de 'Title' preexistente como molde
        fontName='Helvetica-Bold',        # Negrita contundente
        fontSize=24,                      # Texto gigante de titular
        leading=28,                       # Altura de línea para respirabilidad 
        textColor=colors.HexColor("#2A4365"), # Azul corporativo/universitario oscuro
        spaceAfter=20,                    # Empujar todo lo de abajo por 20 ptos
        alignment=1                       # Centrar horizontalmente
    )

    # Estilo de la reseña inicial introductoria
    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,
        textColor=colors.HexColor("#4A5568"), # Matiz de gris, reduce la fatiga contrastante frente a negro
        spaceAfter=20
    )

    # Estilo asignado específicamente para iniciar la subsección de cada Paso en la guía
    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"), # Azul eléctrico claro para atrapar miradas al escanear
        spaceBefore=15,                   # Separa el paso del anterior para denotar nueva acción
        spaceAfter=8
    )

    # Cuerpo textual general: Explicación de cada paso
    body_style = ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        leading=16,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=8
    )

    # Estilo de Terminal Inteligente: Imitación de editores de código (IDE) moderno
    # Aquí aplicamos relleno en caja y colorización oscura.
    code_style = ParagraphStyle(
        name='CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',               # Fuente System Terminal Monospaced
        fontSize=10,
        leading=14,                       # Corto espaciado vertical
        textColor=colors.HexColor("#E2E8F0"), # Blanco-gris plateado (Para visibilidad dentro de la caja oscura)
        backColor=colors.HexColor("#1A202C"), # Gris casi negro
        borderPadding=(8, 10, 8, 10),     # Grosor interno como CSS padding
        borderWidth=1,                    # Establecer anchura real del contorno
        borderColor=colors.HexColor("#2D3748"),
        borderRadius=5,                   # Radio circular moderno
        spaceBefore=5,
        spaceAfter=15
    )
    
    # (Nota: El tag <font name="Courier" color="#D53F8C">comando</font> embebido 
    # en las descripciones funcionará inyectado directamente en el texto string.)

    # 3. Flujo Lógico: Agrupando Información
    # `story` es la tubería secuencial. Le añadiremos cada bloque visual en orden y
    # Platypus se encargará de "dibujarlo" a PDF siguiendo nuestra cascada.
    story = []

    # 3.1. Volcado del Encabezamiento Superior (Subtítulo decorativo en fuente menor)
    story.append(Paragraph("Guía Profesional de Python", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    # 3.2. Volcado del Titular Central Grande
    story.append(Paragraph("Creación de Entornos Virtuales", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # 3.3. Construcción e inserción del sumario inicial
    intro_text = (
        "Un entorno virtual (<b>venv</b>) es una herramienta indispensable en el desarrollo profesional con Python. "
        "Permite mantener las dependencias requeridas por diferentes proyectos completamente aisladas, "
        "evitando conflictos entre versiones de librerías."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Inyección Sistemática del Payload (Contenido real en forma de datos)
    # Mapeo modular estructurado en diccionario permitiendo editar texto fácilmente a futuro.
    steps_data = [
        {
            "title": "1. Verificación de Python",
            "body": "Antes de comenzar, asegúrate de tener Python instalado correctamente en tu sistema. Si lo instalas desde cero, no olvides marcar la opción <b>'Add Python to PATH'</b> en el instalador.",
            "code": "python --version"
        },
        {
            "title": "2. Navegar al Directorio del Proyecto",
            "body": "Abre tu terminal favorita (PowerShell, CMD, o la terminal de VS Code) y navega hasta la carpeta raíz donde residirá tu proyecto.",
            "code": "cd C:\\ruta\\a\\tu\\proyecto"
        },
        {
            "title": "3. Crear el Entorno Virtual",
            # Uso intensivo del Tag Format embebido "<font>" para inyectar estilos de código en bloque de texto fluido.
            "body": "Ejecuta el módulo venv de Python para generar la estructura de carpetas del entorno. Por convención, a esta carpeta se le suele llamar <font name='Courier' color='#D53F8C'>venv</font> o <font name='Courier' color='#D53F8C'>.venv</font>.",
            "code": "python -m venv venv"
        },
        {
            "title": "4. Activar el Entorno",
            "body": "Para empezar a usar el entorno aislado, debes activarlo. El comando varía ligeramente según tu sistema operativo y shell:",
            # Los \n serán transformados luego a etiquetas de quiebre HTML por el código que lee esto.
            "code": "Windows PowerShell:\n.\\venv\\Scripts\\Activate.ps1\n\nWindows CMD:\n.\\venv\\Scripts\\activate.bat\n\nLinux / macOS:\nsource venv/bin/activate"
        },
        {
            "title": "5. Gestión de Dependencias",
            "body": "Una vez activado (verás <font name='Courier' color='#D53F8C'>(venv)</font> en tu terminal), todas las instalaciones usando pip se guardarán exclusivamente en este entorno.",
            "code": "pip install nombre-del-paquete\n\n# Para guardar tus dependencias:\npip freeze > requirements.txt"
        },
        {
            "title": "6. Desactivar el Entorno",
            "body": "Cuando finalices tu trabajo o necesites cambiar a otro proyecto, simplemente desactiva el entorno para volver a la configuración global del sistema.",
            "code": "deactivate"
        }
    ]

    # Iteración Generadora de Pasos: Toma los datos desprovistos de interfaz, y los enlaza junto
    # con los estilos estéticos de ReportLab en nuestra pipeline 'Story'.
    for step in steps_data:
        # Array temporal de empaquetado del Paso N (Contiene su propio Titular + Cuerpo)
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        # Procesamiento en Tiempo de Ejecución (Sanitización del bloque de código)
        # Reemplazamos \n por <br/> ya que la base textual de Paragraph funciona con lógica parecida a un navegador (HTML)
        code_text = step["code"].replace('\n', '<br/>')
        
        # Una vez limpio, encadenamos el bloque de terminal al final en el mismo array temporal.
        step_flowables.append(Paragraph(code_text, code_style))
        
        # Lógica de Inteligencia de Páginas (KeepTogether)
        # "Agarra toda la lista step_flowables, sumale sus alturas;
        # SI SOBREPASA la altura del papel sobrante al pie de página, Pasa todo este sub-bloque junto a la hoja 2."
        story.append(KeepTogether(step_flowables))
        
        # Margen artificial general post-paso concluido.
        story.append(Spacer(1, 10))

    # 5. Adición del Marca de Agua / Pie de Página inferior
    story.append(Spacer(1, 30))
    footer_style = ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=colors.HexColor("#A0AEC0"),
        alignment=1 # Forzar centrado del texto
    )
    story.append(Paragraph("Generado de forma automatizada. Buenas prácticas de desarrollo en Python.", footer_style))

    # 6. Build Engine / Iniciar el render final
    # doc.build() instruye a Reportlab a procesar matemáticamente cada entrada
    # en "story", generar un archivo temporal binario, inyectarle las métricas, trazas e imágenes y escribir
    # un flujo final en formato .pdf al sistema de ficheros dictaminado en el OS local.
    doc.build(story)
    
    # Confirmación por consola de exito y ruta absoluta resuelta (C:\... path) referencial.
    print(f"PDF generado exitosamente en: {os.path.abspath(filename)}")

# Bloque de Validación de Origen
# Ejecutará su código sí y sólo sí este script es iniciado como un target principal
# Ej: 'python script.py'. Evitando la autoejecución accidental de los bloques cuando otro .py hace un import a él.
if __name__ == "__main__":
    create_pdf()
