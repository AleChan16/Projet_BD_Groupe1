set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ok()   { echo -e "${GREEN}  [OK]${NC} $1"; }
fail() { echo -e "${RED}  [FAIL]${NC} $1"; }
info() { echo -e "${BLUE}  [INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}  [WARN]${NC} $1"; }
 
section() {
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  $1${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

# ============================================== 
# Configuration 
# ==============================================

DATA_DIR="./download_data/raw"
DVF_DIR="${DATA_DIR}/dvf"
INSEE_DIR="${DATA_DIR}/insee"
SIRENE_DIR="${DATA_DIR}/sirene"

S3_ALIAS="mysdfs"
S3_DVF_PREFIX="raw-data/dvf"
S3_INSEE_PREFIX="raw-data/insee"
S3_SIRENE_PREFIX="raw-data/sirene"

# URLs des 5 fichiers DVF: chaque fichier correspond à une année (2020, 2021, 2022, 2023, 2024)
# Source: data.gouv.fr - dernière mise à jour: avril 2026

declare -A DVF_FILES=(
    ["dvf_2024.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
    ["dvf_2023.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/99a26050-b94f-4ffc-9eb0-73ed28a895d1"
    ["dvf_2022.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/025b9d29-8efb-40bb-8ce6-5bddf97a4e51"
    ["dvf_2021.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/be6e092d-292a-4568-90bf-4254a261ff3b"
    ["dvf_2020.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/947677ab-ad21-48f4-a9ac-ad217c99cf39"
)

# URL INSEE — communes de France avec population, superficie, coordonnées
# Source: data.gouv.fr — stable et mis à jour annuellement
INSEE_URL="https://www.data.gouv.fr/api/1/datasets/r/dbe8a621-a9c4-4bc3-9cae-be1699c5ff25"
INSEE_FILE="communes_france_2025.csv"

# URL SIRENE 
# Source: data.gouv.fr
SIRENE_URL="https://www.data.gouv.fr/api/1/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576"
SIRENE_FILE="sirene_etablissements.csv"

# ============================================== 
# Vérifications préalables 
# ==============================================

# Installer mc s'il ne l'est pas
if [ ! -f "$HOME/minio-binaries/mc" ]; then
    curl https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs  -o $HOME/minio-binaries/mc
    chmod +x $HOME/minio-binaries/mc
fi
export PATH=$PATH:$HOME/minio-binaries/
mc --version

# Vérifier que unzip est installé
if ! command -v unzip &>/dev/null; then
    fail "unzip non trouvé. Lancez: sudo dnf install -y unzip"
    exit 1
fi
ok "unzip disponible ($(unzip -v 2>&1 | head -1))"

# Vérifier la bonne configuration de l'alias S3
if mc alias list 2>/dev/null | grep -qw "^${S3_ALIAS}"; then
    ok "Alias S3 '${S3_ALIAS}' configuré"
else
    fail "Impossible de configurer l'alias S3. Vérifiez que SeaweedFS est démarré."
    exit 1
fi

# Vérifier que le bucket raw-data a été crée
if ! mc ls ${S3_ALIAS}/raw-data &>/dev/null; then
    info "Bucket raw-data absent, création en cours..."
    mc mb ${S3_ALIAS}/raw-data
    ok "Bucket raw-data créé"
else
    ok "Bucket raw-data disponible"
fi

# Créer les répertoires locaux
mkdir -p "${DVF_DIR}" "${INSEE_DIR}" "${SIRENE_DIR}"
ok "Répertoires locaux créés: ${DVF_DIR}, ${INSEE_DIR}, ${SIRENE_DIR}"

# ============================================== 
# Téléchargement des fichiers DVF 
# ==============================================

section "Téléchargement des fichiers DVF (5 ans)"
 
info "Taille estimée: ~360 Mo compressés, ~1.5 Go décompressés"
info "Cela peut prendre plusieurs minutes selon votre connexion..."
echo ""

DVF_SUCCESS=0
DVF_SKIP=0
DVF_FAIL=0

for FILENAME in "${!DVF_FILES[@]}"; do
    URL="${DVF_FILES[$FILENAME]}"
    FILEPATH="${DVF_DIR}/${FILENAME}"
    TXT_FILE="${FILEPATH%.zip}"
 
    # Vérifier si déjà téléchargé et décompressé
    if [ -f "${TXT_FILE}" ]; then
        warn "${FILENAME%.zip} déjà présent, téléchargement ignoré."
        DVF_SKIP=$((DVF_SKIP + 1))
        continue
    fi
 
    info "Téléchargement de ${FILENAME}..."
    if wget -q --show-progress -O "${FILEPATH}" "${URL}" 2>&1; then
        ok "Téléchargé: ${FILENAME} ($(du -sh "${FILEPATH}" | cut -f1))"
 
        info "Décompression de ${FILENAME}..."
        if unzip -q -o "${FILEPATH}" -d "${DVF_DIR}"; then
            rm "${FILEPATH}"
            # Renommer le fichier extrait avec le nom de l'année
            EXTRACTED=$(ls "${DVF_DIR}"/*.txt 2>/dev/null | head -1)
            if [ -n "${EXTRACTED}" ] && [ "${EXTRACTED}" != "${TXT_FILE}" ]; then
                mv "${EXTRACTED}" "${TXT_FILE}"
            fi
            ok "Décompressé: ${FILENAME%.zip}"
            DVF_SUCCESS=$((DVF_SUCCESS + 1))
        else
            fail "Erreur lors de la décompression de ${FILENAME}"
            DVF_FAIL=$((DVF_FAIL + 1))
        fi
    else
        fail "Erreur lors du téléchargement de ${FILENAME}"
        DVF_FAIL=$((DVF_FAIL + 1))
    fi
done
 
echo ""
info "DVF — Résumé: ${DVF_SUCCESS} téléchargés, ${DVF_SKIP} ignorés, ${DVF_FAIL} erreurs"

# ==============================================
# Téléchargement du fichier INSEE
# ==============================================

INSEE_FILEPATH="${INSEE_DIR}/${INSEE_FILE}"

if [ -f "${INSEE_FILEPATH}" ]; then
    warn "${INSEE_FILE} déjà présent, téléchargement ignoré."
else
    info "Téléchargement du fichier communes INSEE..."
    info "Source: data.gouv.fr — population, superficie, coordonnées par commune"
    if wget -q --show-progress -O "${INSEE_FILEPATH}" "${INSEE_URL}" 2>&1; then
        ok "Téléchargé: ${INSEE_FILE} ($(du -sh "${INSEE_FILEPATH}" | cut -f1))"
    else
        fail "Erreur lors du téléchargement du fichier INSEE"
        fail "URL alternative: https://www.insee.fr/fr/statistiques/fichier/8290591/ensemble.zip"
        exit 1
    fi
fi

# Vérification rapide du contenu
LINE_COUNT=$(wc -l < "${INSEE_FILEPATH}" 2>/dev/null || echo "?")
ok "Fichier INSEE: ${LINE_COUNT} lignes"

# ============================================== 
# Téléchargement du fichier SIRENE 
# ==============================================

SIRENE_FILEPATH="${SIRENE_DIR}/${SIRENE_FILE}"

if [ -f "${SIRENE_FILEPATH}" ]; then
    warn "${SIRENE_FILE} déjà présent, téléchargement ignoré."
else
    info "Téléchargement du fichier SIRENE..."
    info "Source: data.gouv.fr"
    if wget -q --show-progress -O "${SIRENE_FILEPATH}" "${SIRENE_URL}" 2>&1; then
        ok "Téléchargé: ${SIRENE_FILE} ($(du -sh "${SIRENE_FILEPATH}" | cut -f1))"
    else
        fail "Erreur lors du téléchargement du fichier SIRENE"
        exit 1
    fi
fi

# Vérifier si le fichier SIRENE est .zip
if file "${SIRENE_FILEPATH}" | grep -q "Zip archive"; then
    warn "Fichier SIRENE détecté comme ZIP, décompression en cours..."
    mv "${SIRENE_FILEPATH}" "${SIRENE_FILEPATH%.csv}.zip"
    unzip -q "${SIRENE_FILEPATH%.csv}.zip" -d "${SIRENE_DIR}"
    rm "${SIRENE_FILEPATH%.csv}.zip"
    # Renommer le fichier extrait
    EXTRACTED=$(ls "${SIRENE_DIR}"/*.csv 2>/dev/null | head -1)
    if [ -n "${EXTRACTED}" ]; then
        mv "${EXTRACTED}" "${SIRENE_FILEPATH}"
        ok "SIRENE décompressé: $(du -sh "${SIRENE_FILEPATH}" | cut -f1)"
    fi
fi

# Vérification rapide du contenu
LINE_COUNT=$(wc -l < "${SIRENE_FILEPATH}" 2>/dev/null || echo "?")
ok "Fichier SIRENE: ${LINE_COUNT} lignes"


# ==============================================
# Chargement des données dans SeaweedFS (S3) 
# ==============================================

section "Chargement dans SeaweedFS"
 
# Charger les fichiers DVF
info "Chargement des fichiers DVF dans s3://${S3_DVF_PREFIX}/..."
for TXT_FILE in "${DVF_DIR}"/*.txt; do
    if [ -f "${TXT_FILE}" ]; then
        BASENAME=$(basename "${TXT_FILE}")

        # Vérifier si le fichier existe déjà dans S3
        if mc stat "${S3_ALIAS}/${S3_DVF_PREFIX}/${BASENAME}" &>/dev/null; then
            warn "  ${BASENAME} déjà présent dans S3, upload ignoré."
            continue
        fi

        info "  Upload: ${BASENAME} ($(du -sh "${TXT_FILE}" | cut -f1))..."
        if mc cp "${TXT_FILE}" "${S3_ALIAS}/${S3_DVF_PREFIX}/${BASENAME}" 2>/dev/null; then
            ok "  ${BASENAME} chargé dans S3"
        else
            fail "  Erreur lors du chargement de ${BASENAME}"
        fi
    fi
done

# Charger le fichier INSEE
info "Chargement du fichier INSEE dans s3://${S3_INSEE_PREFIX}/..."
if mc stat "${S3_ALIAS}/${S3_INSEE_PREFIX}/${INSEE_FILE}" &>/dev/null; then
    warn "${INSEE_FILE} déjà présent dans S3, upload ignoré."
else
    info "  Upload: ${INSEE_FILE} ($(du -sh "${INSEE_FILEPATH}" | cut -f1))..."
    if mc cp "${INSEE_FILEPATH}" "${S3_ALIAS}/${S3_INSEE_PREFIX}/${INSEE_FILE}" 2>/dev/null; then
        ok "${INSEE_FILE} chargé dans S3"
    else
        fail "Erreur lors du chargement du fichier INSEE"
    fi
fi

# Charger le fichier SIRENE
info "Chargement du fichier INSEE dans s3://${S3_SIRENE_PREFIX}/..."
if mc stat "${S3_ALIAS}/${S3_SIRENE_PREFIX}/${SIRENE_FILE}" &>/dev/null; then
    warn "${SIRENE_FILE} déjà présent dans S3, upload ignoré."
else
    info "  Upload: ${SIRENE_FILE} ($(du -sh "${SIRENE_FILEPATH}" | cut -f1))..."
    if mc cp "${SIRENE_FILEPATH}" "${S3_ALIAS}/${S3_SIRENE_PREFIX}/${SIRENE_FILE}" 2>/dev/null; then
        ok "${SIRENE_FILE} chargé dans S3"
    else
        fail "Erreur lors du chargement du fichier SIRENE"
    fi
fi

# ==============================================
# Vérification finale 
# ==============================================

section "Vérification finale"
 
echo ""
info "Contenu de s3://raw-data/dvf/:"
mc ls ${S3_ALIAS}/raw-data/dvf/ 2>/dev/null || warn "Répertoire DVF vide ou inaccessible"

echo ""
info "Contenu de s3://raw-data/insee/:"
mc ls ${S3_ALIAS}/raw-data/insee/ 2>/dev/null || warn "Répertoire INSEE vide ou inaccessible"

echo ""
info "Contenu de s3://raw-data/sirene/:"
mc ls ${S3_ALIAS}/raw-data/sirene/ 2>/dev/null || warn "Répertoire SIRENE vide ou inaccessible"
 
echo ""
info "Espace utilisé dans SeaweedFS:"
mc du ${S3_ALIAS}/raw-data/ 2>/dev/null || true
 
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Données prêtes pour le pipeline ETL${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Prochaine étape:"
echo "docker exec spark-master spark-submit /scripts/data/etl_data.py"
echo ""




