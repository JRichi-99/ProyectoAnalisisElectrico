import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path
import plotly.graph_objects as go
import numpy as np
from plotly.subplots import make_subplots

# ==========================================
# 0. CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(page_title="Matriz Comercial", layout="wide")
st.title("🎯 Matriz Comercial Interactiva")

# ==========================================
# 1. CARGA DE DATOS
# ==========================================
@st.cache_data
def cargar_datos():
    carpeta_datos = Path(r"D:\ProyectoAnalisisElectrico\PotencialesClientes")
    df = pd.read_parquet(carpeta_datos / "2505_2604_clustered_porcentual_actives_processed.parquet")
    return df

df_base = cargar_datos()

# ==========================================
# 2. PREPARACIÓN DE DATOS BASE
# ==========================================
@st.cache_data
def procesar_dataframe(df):
    df_procesado = df.copy()
    
    # Asegurar numéricos
    cols_numericas = ['potencia_max_horaria', 'medida_count', 'capacidad_carga', 
                      'DEMANDA_CALOR_MWH_sum', 'factor_forma', 'Hora', 'id_cluster']
    for col in cols_numericas:
        if col in df_procesado.columns:
            df_procesado[col] = pd.to_numeric(df_procesado[col], errors='coerce')

    # Cálculos de energía
    df_procesado['energia_max_retirable_kwh'] = df_procesado['potencia_max_horaria'].abs() * 24 * df_procesado['medida_count']
    df_procesado['energia_sobrante_kwh'] = df_procesado['energia_max_retirable_kwh'] * (1 - df_procesado['capacidad_carga'])
    df_procesado['energia_sobrante_mwh'] = df_procesado['energia_sobrante_kwh'] / 1000

    # Filtrar datos válidos y aislar la Hora 0 (para no repetir el mismo cliente 24 veces)
    df_limpio = df_procesado[(df_procesado['energia_sobrante_mwh'] > 0) & (df_procesado['Hora'] == 0)].copy()

    # Calcular el Índice de Saturación (Eje X)
    df_limpio['ratio_calor_sobrante'] = df_limpio['DEMANDA_CALOR_MWH_sum'] / df_limpio['energia_sobrante_mwh']
    
    # Convertir id_cluster a int para visualización limpia (si no es NaN)
    df_limpio['id_cluster'] = df_limpio['id_cluster'].fillna(-1).astype(int)

    return df_limpio

df_limpio = procesar_dataframe(df_base)

# ==========================================
# 3. INTERFAZ DE FILTROS DISTRIBUIDA
# ==========================================
st.markdown("### 🎛️ Selección de Macrozonas y Clústeres")
st.caption("Marca o desmarca los clústeres que deseas visualizar. Los números en :red[ROJO] indican tus Targets de análisis manual.")

# Diccionario de Targets para colorear en rojo
clusters_target = {
    'Norte Grande': [1, 2],
    'Centro': [1, 2, 3],
    'Centro Sur': [1, 2, 4, 5],
    'Sur': [1, 2]
}

# Obtener macrozonas únicas presentes en los datos
macrozonas_presentes = sorted([m for m in df_limpio['macrozona'].dropna().unique() if m != ''])

# Crear columnas distribuidas horizontalmente para las macrozonas
cols_mz = st.columns(len(macrozonas_presentes))
selecciones_clusters = {}

for i, mz in enumerate(macrozonas_presentes):
    with cols_mz[i]:
        st.markdown(f"**{mz}**")
        # Obtener clústeres únicos de esta macrozona
        clusters_mz = sorted([c for c in df_limpio[df_limpio['macrozona'] == mz]['id_cluster'].unique() if c != -1])
        
        selecciones_clusters[mz] = []
        
        # Crear los checkboxes
        for c in clusters_mz:
            # Validar si es target para ponerlo en rojo
            es_target = mz in clusters_target and c in clusters_target[mz]
            
            # Formatear el label con Markdown nativo de Streamlit
            label = f":red[Clúster {c}]" if es_target else f"Clúster {c}"
            
            # Por defecto, marcamos los clústeres target para que la vista inicial sea de tus "mejores"
            if st.checkbox(label, value=es_target, key=f"chk_{mz}_{c}"):
                selecciones_clusters[mz].append(c)

st.markdown("---")
st.markdown("### 🏢 Búsqueda Específica")
col_cliente, col_rut = st.columns(2)

opciones_cliente = df_limpio['CLIENTE'].dropna().unique().tolist()
opciones_rut = df_limpio['RUT_CLIENTE'].dropna().unique().tolist()

with col_cliente:
    filtro_cliente = st.multiselect("Buscar Cliente(s) (Escribe el nombre)", opciones_cliente, default=[])

with col_rut:
    filtro_rut = st.multiselect("Buscar por RUT (Escribe el RUT)", opciones_rut, default=[])

# ==========================================
# 4. APLICAR FILTROS AL DATAFRAME
# ==========================================
# 4.1 Filtrar por combinación de Macrozona + Clúster
mask_mz_cluster = pd.Series(False, index=df_limpio.index)

for mz, clusters_seleccionados in selecciones_clusters.items():
    if clusters_seleccionados:  # Si hay clústeres marcados en esa macrozona
        cond_mz = df_limpio['macrozona'] == mz
        cond_cl = df_limpio['id_cluster'].isin(clusters_seleccionados)
        mask_mz_cluster = mask_mz_cluster | (cond_mz & cond_cl)

df_filtrado = df_limpio[mask_mz_cluster].copy()

# 4.2 Aplicar filtros específicos si el usuario escribió algo
if len(filtro_cliente) > 0:
    df_filtrado = df_filtrado[df_filtrado['CLIENTE'].isin(filtro_cliente)]

if len(filtro_rut) > 0:
    df_filtrado = df_filtrado[df_filtrado['RUT_CLIENTE'].isin(filtro_rut)]

# ==========================================
# 5. CREAR Y MOSTRAR EL GRÁFICO PRINCIPAL
# ==========================================
st.markdown("---")

col_res, col_opt = st.columns([2, 1])
with col_res:
    st.markdown(f"**Resultados:** Se están graficando `{len(df_filtrado)}` puntos (claves únicas).")
with col_opt:
    opcion_color = st.radio("🎨 Colorear burbujas por:", ["Macrozona", "Sector Industrial"], horizontal=True)

if not df_filtrado.empty:
    df_filtrado['id_cluster_str'] = df_filtrado['id_cluster'].astype(str)
    
    if opcion_color == "Macrozona":
        columna_color = 'macrozona'
        mapa_colores = None
    else:
        columna_color = 'SECTOR'
        mapa_colores = {
            'Alimentos': '#2980B9', 'Cemento': '#7F8C8D', 'Comercial, Público, Residencial (CPR)': '#16A085', 
            'Energético': '#C0392B', 'Industrial': '#2C3E50', 'Industrias Varias': '#8E44AD', 
            'Minero': '#B87333', 'Papel y Celulosa': '#27AE60', 'Petroquímica': '#D35400'       
        }

    fig = px.scatter(
        df_filtrado,
        x='ratio_calor_sobrante',
        y='factor_forma',
        size='DEMANDA_CALOR_MWH_sum',
        color=columna_color,
        color_discrete_map=mapa_colores,
        hover_name='CLIENTE',
        custom_data=['clave_unica', 'RUT_CLIENTE', 'CLIENTE'], 
        hover_data={
            'clave_unica': False, # <--- OCULTAMOS LA CLAVE DEL HOVER (Pero sigue en custom_data)
            'RUT_CLIENTE': True, 'REGION_CLIENTE': True, 'SECTOR': True,
            'potencia_max_horaria': ':.2f', 'macrozona': True, 'id_cluster_str': True,
            'factor_forma': ':.2f', 'ratio_calor_sobrante': ':.3f', 'DEMANDA_CALOR_MWH_sum': ':.1f'
        },
        log_x=True, size_max=60,
        labels={'macrozona': 'Macrozona', 'SECTOR': 'Sector Industrial', 'id_cluster_str': 'Clúster'}
    )

    fig.add_vline(x=1, line_dash="dot", line_color="#7F8C8D", annotation_text="Límite Técnico (Ratio=1)")
    
    fig.update_layout(
        plot_bgcolor='white', margin=dict(t=30, b=30),
        xaxis=dict(showgrid=True, gridcolor='lightgrey', title='Índice de Saturación (Demanda / Energía Sobrante) [Log]'),
        yaxis=dict(showgrid=True, gridcolor='lightgrey', title='Factor de Forma (Continuidad)'),
        legend_title_text=opcion_color, height=600
    )

    evento_click = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points")

else:
    evento_click = None
    st.warning("⚠️ No hay datos que coincidan con la selección.")

# ==========================================
# 6. GRÁFICO DE DETALLE (CON DESVIACIÓN ESTÁNDAR Y MÉTRICAS)
# ==========================================
st.markdown("---")

if evento_click and len(evento_click.selection.points) > 0:
    punto_seleccionado = evento_click.selection.points[0]
    
    clave_clic = punto_seleccionado["customdata"][0]
    rut_clic = punto_seleccionado["customdata"][1]
    cliente_clic = punto_seleccionado["customdata"][2]

    st.markdown(f"### 📊 Perfil de Carga Diario: **{cliente_clic}**")
    st.caption(f"🆔 RUT: `{rut_clic}` | 🔑 Clave Única: `{clave_clic}`")
    
    # Filtrar datos de la clave seleccionada
    df_detalle = df_base[df_base['clave_unica'] == clave_clic].copy()
    
    # Conversiones numéricas
    import numpy as np # Asegurarnos de que numpy esté importado para el np.maximum
    df_detalle['Hora'] = pd.to_numeric(df_detalle['Hora'], errors='coerce')
    # Valor absoluto para medida_mean y conversión de desviación
    df_detalle['medida_mean'] = pd.to_numeric(df_detalle['medida_mean'], errors='coerce')*-1
    df_detalle['medida_std'] = pd.to_numeric(df_detalle['medida_std'], errors='coerce')
    df_detalle = df_detalle.sort_values('Hora')

    # Extraer métricas estáticas (tomamos el valor de la primera fila)
    val_tension = df_detalle['tension'].iloc[0]
    val_medida_total = pd.to_numeric(df_detalle['medida_total'].iloc[0], errors='coerce') * -1
    val_pot_max = pd.to_numeric(df_detalle['potencia_max_horaria'].iloc[0], errors='coerce')* -1

    # Extraer variables de calor e información del cliente
    est_nombre = df_detalle['NOMBRE_ESTABLECIMIENTO'].iloc[0]
    sector_ind = df_detalle['SUBSECTOR'].iloc[0]
    calor_sum = pd.to_numeric(df_detalle['DEMANDA_CALOR_MWH_sum'].iloc[0], errors='coerce')
    calor_mean = pd.to_numeric(df_detalle['DEMANDA_CALOR_MWH_mean'].iloc[0], errors='coerce')
    calor_std = pd.to_numeric(df_detalle['DEMANDA_CALOR_MWH_std'].iloc[0], errors='coerce')
    calor_max = pd.to_numeric(df_detalle['DEMANDA_CALOR_MWH_max'].iloc[0], errors='coerce')
    calor_min = pd.to_numeric(df_detalle['DEMANDA_CALOR_MWH_min'].iloc[0], errors='coerce')

    # Mostrar métricas eléctricas arriba
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="⚡ Tensión kV", value=str(val_tension))
    kpi2.metric(label="🔋 Medida Total kWh", value=f"{val_medida_total:,.2f}")
    kpi3.metric(label="📈 Potencia Máx Horaria kW", value=f"{val_pot_max:,.2f}")

    st.write("") # Espacio en blanco

    # CREAMOS DOS COLUMNAS: Izquierda para el gráfico de línea (70%), Derecha para Calor (30%)
    col_chart, col_info = st.columns([7, 3])

    with col_chart:
        # Construimos el gráfico de líneas y bandas
        fig_line = go.Figure()

        # 1. Límite superior de la desviación estándar (invisible)
        fig_line.add_trace(go.Scatter(
            x=df_detalle['Hora'],
            y=df_detalle['medida_mean'] + df_detalle['medida_std'],
            mode='lines',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip'
        ))

        # 2. Límite inferior y relleno (sombra)
        fig_line.add_trace(go.Scatter(
            x=df_detalle['Hora'],
            y=np.maximum(df_detalle['medida_mean'] - df_detalle['medida_std'], 0),
            mode='lines',
            fill='tonexty', 
            fillcolor='rgba(46, 134, 193, 0.2)', 
            line=dict(width=0),
            name='± 1 Desviación Estándar'
        ))

        # 3. Línea principal (Media Horaria)
        fig_line.add_trace(go.Scatter(
            x=df_detalle['Hora'],
            y=df_detalle['medida_mean'],
            mode='lines+markers',
            name='Media Horaria',
            line=dict(color='#2E86C1', shape='spline', width=3),
            hovertemplate="Hora: %{x}<br>Media: %{y:.2f}<extra></extra>"
        ))

        fig_line.update_layout(
            plot_bgcolor='white',
            hovermode="x unified",
            xaxis=dict(
                title="Hora del Día",
                tickmode='linear',
                tick0=0, dtick=1,
                showgrid=True, gridcolor='lightgrey'
            ),
            yaxis=dict(
                title="Consumo Medio (Absoluto)",
                showgrid=True, gridcolor='lightgrey'
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.02,
                xanchor="right", x=1
            ),
            height=450,
            margin=dict(r=10) # Reducimos margen derecho para acercarlo al panel
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with col_info:
        # Panel de Información Térmica
        st.markdown("#### 🏭 Detalle Térmico e Industrial")
        st.info(f"**Establecimiento:** {est_nombre}\n\n**Sector:** {sector_ind}")
        
        # Métrica principal de calor
        st.metric(label="🔥 Demanda Calor Total (Sum)", value=f"{calor_sum:,.1f} MWh")

        # Gráfico de barras para comparar Mínimo, Medio y Máximo
        fig_calor = go.Figure(data=[
            go.Bar(
                x=['Mínimo', 'Media', 'Máximo'],
                y=[calor_min, calor_mean, calor_max],
                marker_color=['#FADBD8', '#E74C3C', '#943126'], # Tonos cálidos (rojos) para representar calor
                text=[f"{calor_min:,.1f}", f"{calor_mean:,.1f}", f"{calor_max:,.1f}"],
                textposition='auto'
            )
        ])

        fig_calor.update_layout(
            plot_bgcolor='white',
            margin=dict(t=20, b=0, l=0, r=0),
            height=200, # Gráfico pequeño
            yaxis=dict(showgrid=True, gridcolor='lightgrey', title="MWh")
        )
        
        st.plotly_chart(fig_calor, use_container_width=True)
        
        # Mostramos la desviación estándar abajo del gráfico como texto de apoyo
        st.caption(f"Desviación Estándar Térmica: **{calor_std:,.2f} MWh**")


    # ==========================================
    # 7. EVOLUCIÓN MENSUAL DINÁMICA CON SANITY CHECK
    # ==========================================
    st.markdown("### 📅 Evolución de Perfiles Mensuales")
    
    # --- SANITY CHECK 1: Validar unicidad en el df_detalle ---
    claves_unicas = df_detalle['clave'].dropna().unique()
    zonas_unicas = df_detalle['Zona'].dropna().unique()

    if len(claves_unicas) > 1 or len(zonas_unicas) > 1:
        st.warning(f"⚠️ **Alerta de Sanidad:** La clave única '{clave_clic}' tiene múltiples claves o zonas asociadas en la base. Se graficará usando la primera encontrada: Clave `{claves_unicas[0]}` | Zona `{zonas_unicas[0]}`.")
    
    # Extraemos los valores validados
    clave_val = claves_unicas[0]
    zona_val = zonas_unicas[0]
    
    @st.cache_data
    def cargar_mes_especifico(mes_str, clave_buscar, zona_buscar):
        ruta = Path(rf"D:\ProyectoAnalisisElectrico\DiaPromedio\Mensuales\{mes_str}\{mes_str}_mean_month.parquet")
        if ruta.exists():
            try:
                df_m = pd.read_parquet(
                    ruta, 
                    engine='pyarrow',
                    filters=[('clave', '==', clave_buscar), ('Zona', '==', zona_buscar)]
                )
                return df_m
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    ruta_base = Path(r"D:\ProyectoAnalisisElectrico\DiaPromedio\Mensuales")
    if ruta_base.exists():
        meses_historial = sorted([d.name for d in ruta_base.iterdir() if d.is_dir()])
    else:
        meses_historial = ["2505", "2506", "2507", "2508", "2509", "2510", "2511", "2512", "2601", "2602", "2603", "2604"]
    
    datos_validos = []
    
    for m in meses_historial:
        df_m = cargar_mes_especifico(m, clave_val, zona_val)
        
        if not df_m.empty:
            # --- SANITY CHECK 2: Validar que solo haya un perfil (sin horas duplicadas) ---
            if len(df_m) > 24 or df_m['Hora'].duplicated().any():
                st.toast(f"⚠️ Mes {m}: Se detectaron horas duplicadas para esta Clave/Zona. Se limpiaron para la visualización.", icon="🛠️")
                df_m = df_m.drop_duplicates(subset=['Hora'])

            tot = pd.to_numeric(df_m['medida_total'].iloc[0], errors='coerce')
            titulo = f"{m} (Tot: {abs(tot):,.0f} kWh)"
            datos_validos.append((titulo, df_m))

    num_meses = len(datos_validos)

    if num_meses > 0:
        cols = 4 if num_meses >= 4 else num_meses
        rows = (num_meses + cols - 1) // cols
        
        titulos_subplots = [t for t, _ in datos_validos]

        fig_grid = make_subplots(
            rows=rows, cols=cols,
            subplot_titles=titulos_subplots,
            shared_xaxes=True,
            shared_yaxes=True, 
            vertical_spacing=0.15 if rows > 1 else 0,
            horizontal_spacing=0.03
        )

        for i, (titulo, df_m) in enumerate(datos_validos):
            r = (i // cols) + 1 
            c = (i % cols) + 1   
            
            df_m['Hora'] = pd.to_numeric(df_m['Hora'], errors='coerce')
            df_m['medida_mean_porcentual'] = pd.to_numeric(df_m['medida_mean_porcentual'], errors='coerce')
            df_m['medida_std_porcentual'] = pd.to_numeric(df_m['medida_std_porcentual'], errors='coerce').fillna(0)*-1
            df_m = df_m.sort_values('Hora')

            # Límite superior 
            fig_grid.add_trace(go.Scatter(
                x=df_m['Hora'],
                y=df_m['medida_mean_porcentual'] + df_m['medida_std_porcentual'],
                mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'
            ), row=r, col=c)
            
            # Límite inferior y sombra
            fig_grid.add_trace(go.Scatter(
                x=df_m['Hora'],
                y=(df_m['medida_mean_porcentual'] - df_m['medida_std_porcentual']).clip(lower=0),
                mode='lines', fill='tonexty', fillcolor='rgba(46, 134, 193, 0.2)', 
                line=dict(width=0), showlegend=False, hoverinfo='skip'
            ), row=r, col=c)
            
            # Línea central media 
            fig_grid.add_trace(go.Scatter(
                x=df_m['Hora'],
                y=df_m['medida_mean_porcentual'],
                mode='lines', line=dict(color='#2E86C1', width=2),
                showlegend=False,
                hovertemplate="Hora: %{x}<br>Media: %{y:.1%}<extra></extra>"
            ), row=r, col=c)

        altura_total = max(300, rows * 220)
        
        fig_grid.update_layout(
            height=altura_total, 
            plot_bgcolor='white',
            margin=dict(t=40, b=20, l=20, r=20)
        )
        
        fig_grid.update_xaxes(showgrid=False, tickmode='linear', dtick=6) 
        fig_grid.update_yaxes(showgrid=False, tickformat=".0%") 

        st.plotly_chart(fig_grid, use_container_width=True)
    else:
        st.info("⚠️ No se encontró historial mensual para este cliente en las carpetas proporcionadas.")

    # ==========================================
    # 8. MAPA DE CALOR: PERFIL HISTÓRICO HORARIO
    # ==========================================
    st.markdown("---")
    st.markdown("### 🌡️ Mapa de Calor: Consumo Horario Histórico")

    @st.cache_data
    def cargar_medidas_horarias(mes_str, clave_buscar, zona_buscar):
        ruta = Path(rf"D:\ProyectoAnalisisElectrico\MedidasValorizadas\{mes_str}\{mes_str}_medidas_horarias.parquet")
        if ruta.exists():
            try:
                # Cargamos SOLO 4 columnas para optimizar memoria
                df_h = pd.read_parquet(
                    ruta, 
                    engine='pyarrow',
                    columns=['clave', 'Zona', 'Fecha_Medicion_last', 'medida_3_sum'],
                    filters=[('clave', '==', clave_buscar), ('Zona', '==', zona_buscar)]
                )
                return df_h
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    ruta_base_valorizadas = Path(r"D:\ProyectoAnalisisElectrico\MedidasValorizadas")
    
    if ruta_base_valorizadas.exists():
        meses_valorizadas = sorted([d.name for d in ruta_base_valorizadas.iterdir() if d.is_dir()])
    else:
        meses_valorizadas = ["2505", "2506", "2507", "2508", "2509", "2510", "2511", "2512", "2601", "2602", "2603", "2604"]
    
    lista_df_horarios = []
    
    with st.spinner('Cargando el historial de horas para el Heatmap...'):
        for m in meses_valorizadas:
            df_mes = cargar_medidas_horarias(m, clave_val, zona_val)
            if not df_mes.empty:
                lista_df_horarios.append(df_mes)

    if len(lista_df_horarios) > 0:
        df_anual = pd.concat(lista_df_horarios, ignore_index=True)
        
        # 1. Convertir la fecha a datetime real
        df_anual['Fecha_Medicion_last'] = pd.to_datetime(df_anual['Fecha_Medicion_last'], errors='coerce')
        df_anual = df_anual.dropna(subset=['Fecha_Medicion_last'])
        
        # 2. Extraer la Hora (Eje Y)
        df_anual['Hora_Heatmap'] = df_anual['Fecha_Medicion_last'].dt.hour
        
        # 3. Calcular día incremental: del 1 al máximo (Eje X)
        fecha_min = df_anual['Fecha_Medicion_last'].dt.date.min()
        df_anual['Dia_Index'] = (df_anual['Fecha_Medicion_last'].dt.date - fecha_min).apply(lambda x: x.days + 1)
        
        # 3.5. Extraer el Periodo (YYMM) y buscar en qué "Dia_Index" empieza cada uno
        # Formateamos la fecha a YYMM (ej: 2505, 2506)
        df_anual['Periodo'] = df_anual['Fecha_Medicion_last'].dt.strftime('%y%m')
        # Agrupamos por Periodo y sacamos el primer día en que aparece
        limites_meses = df_anual.groupby('Periodo')['Dia_Index'].min().sort_values()

        # 4. Formatear medida (multiplicando por -1)
        df_anual['Consumo_kWh'] = pd.to_numeric(df_anual['medida_3_sum'], errors='coerce') * -1
        
        # --- MOSTRAMOS LA CANTIDAD REAL DE DATOS ENCONTRADOS ---
        dias_totales = df_anual['Dia_Index'].max()
        horas_totales = len(df_anual)
        st.caption(f"Visualizando un total de **{horas_totales:,.0f} horas** registradas a lo largo de **{dias_totales} días** de operación.")

        # 5. Crear tabla dinámica (Matriz)
        matriz_hm = df_anual.pivot_table(
            index='Hora_Heatmap', 
            columns='Dia_Index', 
            values='Consumo_kWh', 
            aggfunc='sum'
        )
        
        # 6. Construir el gráfico Heatmap
        fig_hm = go.Figure(data=go.Heatmap(
            z=matriz_hm.values,
            x=matriz_hm.columns,
            y=matriz_hm.index,
            colorscale='Viridis', 
            hovertemplate="Día de operación: %{x}<br>Hora: %{y}:00<br>Consumo: %{z:,.1f} kWh<extra></extra>",
            colorbar=dict(title="kWh")
        ))
        
        # 7. Dibujar líneas verticales punteadas para separar los meses
        for periodo, dia_inicio in limites_meses.items():
            fig_hm.add_vline(
                x=dia_inicio - 0.5, # El -0.5 es para que la línea caiga exactamente entre medio de los píxeles
                line_width=1.5, 
                line_dash="dot", 
                line_color="white", 
                opacity=0.6
            )

        fig_hm.update_layout(
            plot_bgcolor='white',
            xaxis=dict(
                title="Periodos Mensuales",
                tickmode='array',
                tickvals=limites_meses.values, # Posicionamos los textos justo donde empieza el mes
                ticktext=limites_meses.index,  # Escribimos '2505', '2506', etc.
                showgrid=False
            ),
            yaxis=dict(
                title="Hora del Día",
                tickmode='linear',
                tick0=0,
                dtick=1,
                showgrid=False
            ),
            height=450,
            margin=dict(t=30, b=30, l=40, r=20)
        )
        
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("⚠️ No se encontraron registros de medidas horarias para construir el Heatmap.")
else:
    st.info("👆 Haz clic en una burbuja del gráfico de arriba para ver el perfil de 24 horas de esa clave única específica.")