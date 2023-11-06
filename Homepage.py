import streamlit as st
from streamlit_option_menu import option_menu

selected = option_menu (
        menu_title="Homepage",
        options=["Dashboard","Data Harvesting","Data Migration"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal"
        )



if selected =="Dashboard":
    st.title(f"You have selected {selected}")
if selected =="Data Harvesting":
    st.title(f"You have selected {selected}")
if selected =="Data Migration":
    st.title(f"You have selected {selected}")
