# app.py — ANWW Duikapp (login via username + wachtwoord) • Build v2025-10-20
# Functies: Activiteitenkalender (met inschrijven + tot 3 maaltijdkeuzes + opmerking),
# Afrekening (alleen duikers tellen mee), Duiken invoeren/overzicht, Beheer (leden),
# Wekelijkse mail preview/export (eerstvolgende 4 activiteiten).
#
# Minimale, gerichte aanpassingen in deze versie:
# - BREVET_CHOICES exact: ['k1ster','1ster','2ster','3ster','4ster','ass-inst','1*inst','2*inst','3*inst']
# - canon_brevet/normalize_brevet mappen varianten naar deze 9 waarden
# - Datumselectie bij activiteit: dag/maand/jaar (DD/MM/YYYY)
# - Opmerkingenveld bij activiteit + mee in export/afdruk
# - Duiken: overnemen van datum/plaats/duikers uit activiteit (indien beschikbaar)
# - Verwijderd: dubbele st.date_input('Datum*') om StreamlitDuplicateElementId te vermijden

import streamlit as st
import requests
import pandas as pd
import datetime
from datetime import datetime as dt
import io
import os
import time
import math
import hmac, hashlib, secrets
from typing import Optional
from supabase import create_client, Client
from postgrest.exceptions import APIError
import httpx

st.set_page_config(page_title="ANWW Duikapp", layout="wide")
APP_BUILD = "v2025-10-20"

def inject_css():
    st.markdown("""
    <style>
      :root { --background: #8DAEBA; --secondary: #A38B16; --text: #11064D; --primary: #728DCC; --border: #2a355a; --success: #3CA133; --warning: #f59e0b; --error: #ef4444; }
      .stApp, [data-testid="stAppViewContainer"], section.main, div.block-container { background-color: var(--background) !important; color: var(--text) !important; }
      section[data-testid="stSidebar"], [data-testid="stSidebarContent"] { background-color: var(--secondary) !important; }
      .stTabs [data-baseweb="tab"] { background: var(--secondary) !important; color: #fff !important; border-radius: 5px 5px 0 0; font-weight: 600; }
      .stTabs [aria-selected="true"] { background: var(--primary) !important; color: white !important; }
      .stExpander { border: 1px solid var(--border) !important; background: #ffffff80 !important; }
      .stButton > button, .stDownloadButton > button { background-color: var(--primary) !important; color: #fff !important; border: 2px solid var(--border) !important; border-radius: 10px !important; [...]
      .stButton > button:hover, .stDownloadButton > button:hover { filter: brightness(1.1) !important; transform: translateY(-1px); }
      .stButton.success > button { background: var(--success) !important; border-color: var(--success) !important; }
      .stButton.warning > button { background: var(--warning) !important; color:#000 !important; border-color: var(--warning) !important; }
      .stButton.error   > button { background: var(--error)   !important; border-color: var(--error)   !important; }
      [data-testid="stDataFrame"] thead tr th { background: #ffffff88 !important; color: var(--text) !important; }
      hr, .stDivider { border-top: 2px solid var(--border) !important; }
      h1, h2, h3, h4, h5 { color: var(--text) !important; }
    </style>
    """, unsafe_allow_html=True)
inject_css()

def is_readonly() -> bool:
    return bool(st.secrets.get("app", {}).get("force_readonly", False))

@st.cache_resource
def get_client() -> Client:
    supa = st.secrets.get("supabase", {})
    url = supa.get("url") or os.getenv("SUPABASE_URL")
    key = supa.get("anon_key") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        st.error("Supabase credentials ontbreken. Zet [supabase].url en anon_key in secrets.toml.")
        st.stop()
    return create_client(url, key)

sb: Client = get_client()

def run_db(fn, *, what="db call", tries=2, backoff=0.4):
    for i in range(tries):
        try:
            return fn(sb)
        except (httpx.ConnectError, httpx.ReadTimeout) as e:
            if i+1 < tries:
                time.sleep(backoff * (i+1))
                continue
            st.error(f"Netwerkfout bij {what}: {e}"); st.stop()
        except APIError as e:
            st.error(f"API-fout bij {what}. Controleer tabellen/RLS/policies (anon).");
            st.caption(str(e)); st.stop()
        except Exception as e:
            if i+1 < tries:
                time.sleep(backoff * (i+1))
                continue
            st.error(f"Onverwachte fout bij {what}: {e}"); st.stop()

def _hash_password(password: str, iterations: int = 240000) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2${iterations}${salt.hex()}${dk.hex()}"

def _verify_password(password: str, hashed: str) -> bool:
    try:
        algo, iters, salt_hex, hash_hex = hashed.split("$", 3)
        if algo != "pbkdf2":
            return False
        iters = int(iters)
        salt = bytes.fromhex(salt_hex)
        ref = bytes.fromhex(hash_hex)
        test = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
        return hmac.compare_digest(ref, test)
    except Exception:
        return False

def auth_get_user_by_username(login: str) -> Optional[dict]:
    login = (login or "").strip()
    if not login:
        return None
    res = run_db(lambda c: c.table("auth_local").select("*").eq("username", login).limit(1).execute(),
                 what="auth_local select by username")
    rows = res.data or []
    if rows:
        return rows[0]
    if "@" in login:
        res2 = run_db(lambda c: c.table("auth_local").select("*").eq("email", login.lower()).limit(1).execute(),
                      what="auth_local select by email")
        rows2 = res2.data or []
        if rows2:
            return rows2[0]
    return None

def auth_count_admins() -> int:
    res = run_db(lambda c: c.table("auth_local").select("id", count="exact").eq("role","admin").execute(),
                 what="auth_local count admin")
    return res.count or 0

def auth_create_user(username: str, password: str, role: str, email: str | None = None):
    if is_readonly():
        raise Exception("Read-only modus")
    role = (role or "viewer").strip().lower()
    if role not in {"admin","user","member","viewer"}:
        role = "viewer"
    payload = {
        "username": (username or "").strip(),
        "email": (email or "").strip().lower() or None,
        "password_hash": _hash_password(password),
        "role": role,
    }
    run_db(lambda c: c.table("auth_local").insert(payload).execute(), what="auth_local insert")

def auth_update_password_by_username(username: str, new_pw: str):
    if is_readonly():
        raise Exception("Read-only modus")
    run_db(lambda c: c.table("auth_local").update({"password_hash": _hash_password(new_pw)}).eq("username", username).execute(),
           what="auth_local update password")

def current_user():
    return st.session_state.get("auth_user") or {}

def current_role() -> str:
    u = current_user()
    r = (u.get("role") or "viewer").lower()
    return r if r in {"admin","user","member","viewer"} else "viewer"

def current_username() -> str:
    return (current_user().get("username") or "").strip()

def current_email() -> str:
    return (current_user().get("email") or "").strip().lower()

BREVET_CHOICES = ['k1ster','1ster','2ster','3ster','4ster','ass-inst','1*inst','2*inst','3*inst']

def normalize_brevet(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"", "(geen)", "geen", "none", "null"}:
        return None
    import re as _re
    s = _re.sub(r'[-–—‒−]', '-', s)
    s = _re.sub(r'[＊∗⋆✱✳︎]', '*', s)
    s = s.replace(" ", "")
    s = s.replace("instructeur","inst").replace("assistent","ass")
    if s in {"k1","k1ster","kand1","kand1ster","kandidaat1ster"}: return "k1ster"
    if s in {"1","1ster","1*"}: return "1ster"
    if s in {"2","2ster","2*"}: return "2ster"
    if s in {"3","3ster","3*"}: return "3ster"
    if s in {"4","4ster","4*"}: return "4ster"
    if s in {"assinst","ass-inst","assinst.","assinstr"}: return "ass-inst"
    if s in {"1inst","1*inst","1-inst","1instructeur","1*instructeur"}: return "1*inst"
    if s in {"2inst","2*inst","2-inst","2instructeur","2*instructeur"}: return "2*inst"
    if s in {"3inst","3*inst","3-inst","3instructeur","3*instructeur"}: return "3*inst"
    return s

def canon_brevet(v):
    if v is None:
        return None
    s = normalize_brevet(v)
    if s is None:
        return None
    xmap = {"assinst":"ass-inst","1inst":"1*inst","2inst":"2*inst","3inst":"3*inst","k1":"k1ster"}
    s = xmap.get(s, s)
    return s if s in BREVET_CHOICES else None

ROLE_CHOICES = ['admin','user','member','viewer']


def leden_upsert(payload: dict):
    if is_readonly():
        raise Exception("Read-only modus")
    p = dict(payload)
    if "duikbrevet" in p:
        val = p.get("duikbrevet")
        p["duikbrevet"] = None if val is None else canon_brevet(val)
    if "email" in p:
        em = (p.get("email") or "").strip()
        p["email"] = em.lower() if em else None

    # NOTE:
    # Postgres upsert requires that 'on_conflict' matches an existing UNIQUE/PK constraint.
    # Many schemas have *separate* unique constraints (e.g. UNIQUE(email), UNIQUE(username)),
    # so 'email,username' is invalid unless there is a composite unique index.
    # We avoid the ON CONFLICT error by doing a small merge routine:
    # 1) If 'id' is present, upsert by primary key 'id'.
    # 2) Else, try to find an existing row by username, then by email; if found, update by id.
    # 3) If nothing exists, insert a new row.
    def _select_id_by(field: str, value: str):
        if not value:
            return None
        res = run_db(lambda c: c.table("leden").select("id").eq(field, value).limit(1).execute(), what=f"leden select by {field}")
        rows = (res.data or []) if hasattr(res, "data") else []
        return rows[0].get("id") if rows else None

    # Case 1: Upsert by primary key if we have it
    if p.get("id"):
        run_db(lambda c: c.table("leden").upsert(p, on_conflict="id").execute(), what="leden upsert by id")
        return

    # Case 2: Try to match existing by username/email and update
    uid = None
    if p.get("username"):
        uid = _select_id_by("username", p.get("username"))
    if not uid and p.get("email"):
        uid = _select_id_by("email", p.get("email"))

    if uid:
        run_db(lambda c: c.table("leden").update(p).eq("id", uid).execute(), what="leden update by id (merge)")
    else:
        run_db(lambda c: c.table("leden").insert(p).execute(), what="leden insert")

def leden_list_df() -> pd.DataFrame:
    res = run_db(lambda c: c.table("leden").select("*").order("achternaam").order("voornaam").execute(),
                 what="leden select")
    return pd.DataFrame(res.data or [])

def leden_get_by_email(email: str) -> Optional[dict]:
    email = (email or "").lower().strip()
    if not email:
        return None
    res = run_db(lambda c: c.table("leden").select("*").eq("email", email).limit(1).execute(),
                 what="leden by email")
    rows = res.data or []
    return rows[0] if rows else None

def leden_get_by_username(username: str) -> Optional[dict]:
    if not username:
        return None
    res = run_db(lambda c: c.table("leden").select("*").eq("username", username).limit(1).execute(),
                 what="leden by username")
    rows = res.data or []
    return rows[0] if rows else None

def duikers_labels() -> list[str]:
    res = run_db(lambda c: c.table("duikers").select("voornaam, achternaam, naam").execute(),
                 what="duikers select")
    rows = res.data or []
    out = []
    for r in rows:
        vn, an = (r.get("voornaam") or "").strip(), (r.get("achternaam") or "").strip()
        out.append(f"{an}, {vn}".strip(", ") if (vn or an) else (r.get("naam") or "").strip())
    def key(x):
        if "," in x:
            an, vn = [p.strip() for p in x.split(",", 1)]
            return (an.lower(), vn.lower())
        parts = x.split()
        return (parts[-1].lower() if parts else "", " ".join(parts[:-1]).lower())
    return sorted([o for o in out if o], key=key)

def plaatsen_list() -> list[str]:
    res = run_db(lambda c: c.table("duikplaatsen").select("plaats").order("plaats").execute(),
                 what="duikplaatsen select")
    return [r["plaats"] for r in (res.data or [])]

def plaats_add(plaats: str):
    if is_readonly():
        raise Exception("Read-only modus")
    run_db(lambda c: c.table("duikplaatsen").insert({"plaats": plaats}).execute(),
           what="duikplaatsen insert")

def duiken_insert(rows: list[dict]):
    if is_readonly():
        raise Exception("Read-only modus")
    if rows:
        run_db(lambda c: c.table("duiken").insert(rows).execute(), what="duiken insert")

def duiken_fetch_df() -> pd.DataFrame:
    res = run_db(lambda c: c.table("duiken").select("*").order("datum", desc=True).order("plaats").order("duiker").execute(),
                 what="duiken select")
    return pd.DataFrame(res.data or [])

def duiken_delete_by_ids(ids: list):
    if is_readonly():
        raise Exception("Read-only modus")
    if ids:
        run_db(lambda c: c.table("duiken").delete().in_("id", ids).execute(), what="duiken delete")

def afrekening_insert(row: dict):
    if is_readonly():
        raise Exception("Read-only modus")
    run_db(lambda c: c.table("afrekeningen").insert(row).execute(), what="afrekeningen insert")

def activiteit_add(titel, omschr, datum, tijd, locatie, meal_opts, created_by, opmerkingen=None):
    if is_readonly():
        raise Exception("Read-only modus")
    payload = {
        "titel": titel.strip(),
        "omschrijving": (omschr or "").strip(),
        "datum": datum.isoformat(),
        "tijd": tijd.isoformat() if tijd else None,
        "locatie": (locatie or "").strip() or None,
        "meal_options": meal_opts or None,
        "created_by": created_by or None,
        "opmerkingen": (opmerkingen or "").strip() or None
    }
    run_db(lambda c: c.table("activiteiten").insert(payload).execute(), what="activiteiten insert")

def activiteit_update(activiteit_id: str, payload: dict):
    if is_readonly():
        raise Exception("Read-only modus")
    run_db(lambda c: c.table("activiteiten").update(payload).eq("id", activiteit_id).execute(),
           what="activiteiten update")

def signups_delete_users(activiteit_id: str, usernames: list[str] | None = None, lid_ids: list[str] | None = None):
    if is_readonly():
        raise Exception("Read-only modus")
    def _go(c):
        q = c.table("activity_signups").delete().eq("activiteit_id", activiteit_id)
        if usernames:
            q = q.in_("username", usernames)
        if lid_ids:
            q = q.in_("lid_id", lid_ids)
        return q.execute()
    return _go


def activiteiten_list_df(upcoming=True) -> pd.DataFrame:
    def _go(c):
        q = c.table("activiteiten").select("*")
        if upcoming:
            q = q.gte("datum", datetime.date.today().isoformat())
        return q.order("datum").order("tijd").execute()
    res = run_db(_go, what="activiteiten select")
    return pd.DataFrame(res.data or [])

def signups_get(activiteit_id: str) -> pd.DataFrame:
    res = run_db(lambda c: c.table("activity_signups").select("*").eq("activiteit_id", activiteit_id).order("signup_ts").execute(),
                 what="signups select")
    df = pd.DataFrame(res.data or [])
    for col in ["id","activiteit_id","username","lid_id","status","eating","meal_choice","opmerking","signup_ts"]:
        if col not in df.columns:
            df[col] = None
    try:
        df["signup_ts"] = pd.to_datetime(df["signup_ts"], errors="coerce")
    except Exception:
        pass
    return df

def signup_upsert(activiteit_id: str, username: str | None, lid_id: str | None,
                  status: str, eating: bool | None, meal_choice: str | None, opmerking: str | None = None):
    if is_readonly():
        raise Exception("Read-only modus")
    assert status in ("yes","no")
    def _lookup(c):
        q = c.table("activity_signups").select("id").eq("activiteit_id", activiteit_id)
        if username: q = q.eq("username", username)
        if lid_id: q = q.eq("lid_id", lid_id)
        return q.limit(1).execute()
    found = run_db(_lookup, what="signups find")
    rows = found.data or []
    payload = {
        "activiteit_id": activiteit_id,
        "status": status,
        "eating": bool(eating) if eating is not None else None,
        "meal_choice": (meal_choice or "").strip() or None,
        "username": username or None,
        "lid_id": lid_id or None,
        "opmerking": (opmerking or "").strip() or None,
        "signup_ts": dt.utcnow().isoformat()
    }
    if rows:
        sid = rows[0]["id"]
        run_db(lambda c: c.table("activity_signups").update(payload).eq("id", sid).execute(), what="signups update")
    else:
        run_db(lambda c: c.table("activity_signups").insert(payload).execute(), what="signups insert")

def appbar(tag: str):
    col1, col2, col3 = st.columns([5, 3, 2])
    with col1:
        st.markdown("**ANWW Duikapp**")
    with col2:
        st.markdown(f"<div class='badge'>{current_username() or '—'} · {current_role()} · Build {APP_BUILD}</div>", unsafe_allow_html=True)
    with col3:
        if st.button("Uitloggen", key=f"logout_{tag}"):
            st.session_state.pop("auth_user", None)
            st.rerun()

def require_role(*allowed):
    if current_role() not in allowed:
        st.error("Onvoldoende rechten."); st.stop()

def page_setup_first_admin_username():
    st.title("Eerste admin aanmaken")
    st.info("Er bestaat nog geen admin. Maak eerst de eerste admin aan.")
    with st.form("first_admin"):
        username = st.text_input("Login (gebruikersnaam)", placeholder="vb. d.verbraeken")
        pw1 = st.text_input("Wachtwoord", type="password")
        pw2 = st.text_input("Herhaal wachtwoord", type="password")
        email = st.text_input("E-mail (optioneel)")
        submitted = st.form_submit_button("Maak admin", type="primary")
    if submitted:
        if not username or not pw1 or len(pw1) < 5 or pw1 != pw2:
            st.error("Controleer login en wachtwoord (min. 5 tekens en gelijk).");
            return
        try:
            auth_create_user(username=username, password=pw1, role="admin", email=email or None)
            payload = {
                "voornaam": "",
                "achternaam": "",
                "email": (email or "").strip().lower() or f"{username}@local",
                "username": username,
                "role": "admin",
                "opt_in_weekly": True,
                "actief": True
            }
            leden_upsert(payload)
            st.success("Admin aangemaakt. Je kan nu inloggen.")
            st.session_state["just_created_admin"] = True
        except Exception as e:
            st.error(f"Mislukt: {e}")

def page_login_username():
    st.title("Inloggen")
    if st.session_state.get("just_created_admin"):
        st.success("Admin aangemaakt. Log nu in."); st.session_state.pop("just_created_admin", None)
    with st.form("login_form"):
        login = st.text_input("Login (gebruikersnaam)")
        pw = st.text_input("Wachtwoord", type="password")
        submitted = st.form_submit_button("Login", type="primary")
    if not submitted: return
    user = auth_get_user_by_username(login)
    if not user or not _verify_password(pw, user.get("password_hash") or ""):
        st.error("Onjuiste login."); return
    st.session_state["auth_user"] = {"id": user.get("id"), "username": user.get("username"), "email": user.get("email"), "role": user.get("role")}
    st.success("Ingelogd."); st.rerun()



def page_leden():
    """Overzicht van leden. Admin kan bewerken; user/member ziet enkel lijst.
       'username' en 'role' enkel zichtbaar voor admin.
    """
    st.header("Leden")
    try:
        df = leden_list_df()
    except Exception as e:
        st.error(f"Kon ledenlijst niet laden: {e}")
        return
    if df is None or df.empty:
        st.info("Nog geen leden.")
        return

    rol = current_role()
    base_cols = ["voornaam","achternaam","email","brevet","telefoon","adres","geboortedatum"]
    admin_extra = ["username","role"]
    zicht_cols = [c for c in base_cols if c in df.columns]
    if rol == "admin":
        zicht_cols += [c for c in admin_extra if c in df.columns]
    if not zicht_cols:
        zicht_cols = list(df.columns)
    view = df[zicht_cols].copy()

    # Zoekveld
    zoek = st.text_input("🔎 Zoek in leden", value=st.session_state.get("leden_filter",""), key="leden_filter", placeholder="Naam, e-mail, brevet, telefoon, adres...")
    if isinstance(zoek, str) and zoek.strip():
        q = zoek.strip().lower()
        mask = pd.Series(False, index=view.index)
        for col in view.columns:
            mask = mask | view[col].astype(str).str.lower().str.contains(q, na=False)
        view = view[mask]

    # Geboortedatum formatteren
    def _fmt_nl_date(val):
        try:
            dt = pd.to_datetime(val, errors="coerce")
            if pd.isna(dt):
                return ""
            months = ["januari","februari","maart","april","mei","juni","juli","augustus","september","oktober","november","december"]
            return f"{dt.day:02d}/{months[int(dt.month)-1]}/{dt.year}"
        except Exception:
            return ""

    if "geboortedatum" in view.columns:
        view["geboortedatum"] = view["geboortedatum"].apply(_fmt_nl_date)

    sort_cols = [c for c in ["achternaam","voornaam"] if c in view.columns]
    if sort_cols:
        view = view.sort_values(sort_cols, na_position="last")

    st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()

    if rol == "admin":
        st.subheader("Profiel bewerken (admin)")
        opties = []
        for _, r in df.iterrows():
            label = f"{r.get('voornaam','')} {r.get('achternaam','')}".strip() or r.get('username','') or "lid"
            opties.append((label, r.to_dict()))
        labels = [o[0] for o in opties]
        if not labels:
            st.info("Geen leden beschikbaar om te bewerken.")
            return
        idx = st.selectbox("Kies lid", list(range(len(labels))), format_func=lambda i: labels[i], key="leden_edit_pick_admin")
        data = opties[idx][1]

        col1, col2 = st.columns(2)
        with col1:
            v_voor = st.text_input("Voornaam", value=str(data.get("voornaam") or ""), key="leden_e_vn")
            v_naam = st.text_input("Achternaam", value=str(data.get("achternaam") or ""), key="leden_e_an")
            v_email = st.text_input("E-mail", value=str(data.get("email") or ""), key="leden_e_em")
            v_brevet = st.text_input("Brevet", value=str(data.get("brevet") or ""), key="leden_e_bv")
        with col2:
            v_tel = st.text_input("Telefoon", value=str(data.get("telefoon") or ""), key="leden_e_tel")
            v_adres = st.text_area("Adres", value=str(data.get("adres") or ""), key="leden_e_adr")
            v_geboorte = st.text_input("Geboortedatum (YYYY-MM-DD)", value=str(data.get("geboortedatum") or ""), key="leden_e_gbd")

        if st.button("Bewaar wijzigingen", key="leden_save_btn"):
            try:
                fields = {"voornaam": v_voor.strip(), "achternaam": v_naam.strip(), "email": v_email.strip(), "brevet": v_brevet.strip(),
                          "telefoon": v_tel.strip(), "adres": v_adres.strip(), "geboortedatum": v_geboorte.strip()}
                ident = {"id": data["id"]} if ("id" in data and pd.notna(data.get("id"))) else ({"username": data.get("username")} if data.get("username") else {})
                if not ident:
                    raise RuntimeError("Geen sleutel (id/username) voor update.")
                leden_update_fields(ident, fields)
                st.success("Profiel opgeslagen.")
                st.rerun()
            except Exception as ex:
                st.error(f"Opslaan mislukt: {ex}")


def page_profiel():
    appbar("profiel")
    st.header("Mijn profiel")
    my_username = st.session_state.get("demo_username") or st.session_state.get("username")
    if not my_username:
        st.info("Geen username gevonden in sessie.")
        return
    try:
        df = leden_list_df()
    except Exception as e:
        st.error(f"Kon ledenlijst niet laden: {e}")
        return
    if df is None or df.empty or "username" not in df.columns:
        st.info("Ledenlijst niet beschikbaar.")
        return
    me = df[df["username"].astype(str) == str(my_username)]
    if me.empty:
        st.info("Je profiel werd niet gevonden op basis van je username.")
        return
    data = me.iloc[0].to_dict()
    col1, col2 = st.columns(2)
    with col1:
        v_voor = st.text_input("Voornaam", value=str(data.get("voornaam") or ""), key="prof_vn")
        v_naam = st.text_input("Achternaam", value=str(data.get("achternaam") or ""), key="prof_an")
        v_email = st.text_input("E-mail", value=str(data.get("email") or ""), key="prof_em")
        v_brevet = st.text_input("Brevet", value=str(data.get("brevet") or ""), key="prof_bv")
    with col2:
        v_tel = st.text_input("Telefoon", value=str(data.get("telefoon") or ""), key="prof_tel")
        v_adres = st.text_area("Adres", value=str(data.get("adres") or ""), key="prof_adr")
        v_geboorte = st.text_input("Geboortedatum (YYYY-MM-DD)", value=str(data.get("geboortedatum") or ""), key="prof_gbd")
        def _fmt_nl_date(val):
            try:
                dt = pd.to_datetime(val, errors="coerce")
                if pd.isna(dt):
                    return ""
                months = ["januari","februari","maart","april","mei","juni","juli","augustus","september","oktober","november","december"]
                return f"{dt.day:02d}/{months[int(dt.month)-1]}/{dt.year}"
            except Exception:
                return ""
        _preview = _fmt_nl_date(v_geboorte)
        st.caption(f"Weergave: {_preview if _preview else '–'}")
    if st.button("Bewaar mijn gegevens", key="prof_save_btn"):
        try:
            fields = {"voornaam": v_voor.strip(), "achternaam": v_naam.strip(), "email": v_email.strip(), "brevet": v_brevet.strip(),
                      "telefoon": v_tel.strip(), "adres": v_adres.strip(), "geboortedatum": v_geboorte.strip()}
            ident = {"username": my_username}
            leden_update_fields(ident, fields)
            st.success("Profiel opgeslagen.")
            st.rerun()
        except Exception as ex:
            st.error(f"Opslaan mislukt: {ex}")

if __name__ == "__main__":
    main()

# --- Settings helpers (weekplanner toggle) ---
def settings_get_bool(key: str, default: bool = False) -> bool:
    try:
        res = run_db(lambda c: c.table("settings").select("value_bool").eq("key", key).maybe_single().execute(),
                     what="settings get")
        data = getattr(res, "data", None) or {}
        val = data.get("value_bool", None)
        return bool(val) if val is not None else default
    except Exception:
        return default

def settings_set_bool(key: str, value: bool) -> None:
    if is_readonly():
        return
    run_db(lambda c: c.table("settings").upsert({"key": key, "value_bool": bool(value)}).execute(),
           what="settings set")


def weekmail_test_send(to_email: str) -> tuple[bool, str]:
    """Call Supabase Edge Function 'weekmail' in dry-run mode to send only to 'to_email'."""
    url = (SUPABASE_URL.rstrip('/') + "/functions/v1/weekmail") if SUPABASE_URL else ""
    if not url or not SUPABASE_ANON_KEY:
        return False, "SUPABASE_URL/ANON_KEY ontbreekt in environment."
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json",
            },
            json={"dryRun": True, "to": to_email},
            timeout=30,
        )
        if resp.status_code >= 400:
            return False, f"HTTP {resp.status_code}: {resp.text[:300]}"
        data = resp.json()
        if not data.get("ok"):
            return False, f"Mislukt: {data}"
        return True, "Testmail verstuurd."
    except Exception as ex:
        return False, f"Fout bij aanroepen Edge Function: {ex}"

def leden_update_fields(identifier: dict, fields: dict):
    """Update velden in TBL_LEDEN."""
    if is_readonly():
        raise RuntimeError("App staat in read-only modus.")
    if not isinstance(identifier, dict) or not identifier:
        raise ValueError("Identifier ontbreekt.")
    def _q(c):
        q = c.table(TBL_LEDEN).update(fields)
        for k, v in identifier.items():
            q = q.eq(k, v)
        return q.execute()
    run_db(_q, what="leden_update_fields")