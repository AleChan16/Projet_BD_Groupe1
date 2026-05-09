echo "Lancement des conteneurs..."

# ==== Vérification préalables du répertoire data/grafana avec le bon propriétaire (472:472) ====
if [ ! -d "data/grafana" ]; then
    echo "Vous devez lancer le script de préparation des données"
    echo " (sudo ../data_repertoires_droits.sh) "
    echo "avant de démarrer les services."
    exit -1
fi
if [ "$(stat -c '%u' data/grafana)" != "472" ]; then
    echo "Vous devez corriger le propriétaire de data/grafana (uid 472) avant de démarrer les services."
    exit -1
fi
echo "  data/grafana OK (uid=$(stat -c '%u' data/grafana))"

# ==== Installation de mc s'il n'est pas déjà installé ====
if [ ! -f "$HOME/minio-binaries/mc" ]; then
    curl https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs  -o $HOME/minio-binaries/mc
    chmod +x $HOME/minio-binaries/mc
fi
export PATH=$PATH:$HOME/minio-binaries/
mc --version

# ==== SeaweedFS ====
# Démarrage de SeaweedFS Master + SeaweedFS Volume + SeaweedFS Filer
docker compose up -d seaweedfs-master && sleep 3 && docker compose up -d seaweedfs-volume && sleep 3 && docker compose up -d seaweedfs-filer 2>&1
echo "Attente de 10s pour que le stockage S3 soit opérationnel..."
sleep 10

# ==== Vérification du cluster SeaweedFS ====
curl -s http://localhost:9333/dir/status | python3 -c "
import json,sys
d = json.load(sys.stdin)
centers = d.get('Topology', {}).get('DataCenters') or []
for c in centers:
    for r in c.get('Racks',[]):
        for n in r.get('DataNodes',[]):
            print(f\"  {n['Url']}: volumes={n['Volumes']}, max={n['Max']}\")"

# ==== Création de l'alias "mysdfs" ====
mc alias set mysdfs http://localhost:8333 admin adminpass 2>/dev/null
until mc alias list 2>/dev/null | grep -qw "^mysdfs"; do
    echo "Alias mysdfs non disponible, nouvelle tentative..."
    mc alias set mysdfs http://localhost:8333 admin adminpass 2>/dev/null
    sleep 2
done
echo "Alias mysdfs crée et verifié."
echo "Création des buckets warehouse et raw-data"

# ==== Création du bucket warehouse et raw-data s'ils n'existent pas déjà ====
for BUCKET in warehouse raw-data; do
    # Essayer de créer le bucket et enregistrer une eventuelle erreur
    RESULT=$(mc mb mysdfs/${BUCKET} 2>&1)
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "Bucket ${BUCKET} créé avec succès."
    elif echo "$RESULT" | grep -qi "already exists\|already own"; then
        echo "Bucket ${BUCKET} existe déjà — OK."
    else
        # En cas d'erreur, afficher le message de diagnostic
        echo "ERREUR lors de la création du bucket ${BUCKET}:"
        echo "  $RESULT"
        echo "Vérifiez que seaweedfs-filer est bien démarré."
    fi

    # Vérification finale, quel que soit le résultat précédent
    if mc ls mysdfs 2>/dev/null | grep -qw "${BUCKET}"; then
        echo "Bucket ${BUCKET} vérifié dans SeaweedFS."
    else
        echo "ERREUR: bucket ${BUCKET} introuvable dans SeaweedFS après création."
    fi
done

# ==== Démarrage de PostgreSQL + Hive Metastore ====
docker compose up -d postgres 2>&1 && echo "Attente de 8s pour que PostgreSQL soit opérationnel..." && sleep 15 && docker compose up -d metastore 2>&1

# Après premier lancement, on change la valeur de IS_RESUME: "true" dans le fichier .env pour éviter de réinitialiser les données à chaque lancement
if [ -f ".env" ] && grep -q "IS_RESUME=false" .env; then
    sed -i 's/IS_RESUME=false/IS_RESUME=true/' .env
    echo "Premier lancement détecté, mise à jour de IS_RESUME=true dans le fichier .env pour préserver les données lors des prochains démarrages."
fi

# ==== Démarrage d'OpenSearch + OpenSearch Dashboards ====
docker compose up -d opensearch && echo "Attente de 30s pour qu'OpenSearch soit operationnel..." && sleep 30 && docker compose up -d opensearch-dashboards 2>&1

# ==== Spark ====
# Construction de l'image Spark uniquement si elle n'existe pas
if ! docker image inspect spark-projet &>/dev/null; then
    echo "Image spark-projet non trouvée, construction en cours..."
    docker compose build spark-master 2>&1 && echo "Image Spark construite avec succes"
else
    echo "Image spark-projet déjà présente, build ignore."
fi

# Démarrage de Spark Master
docker compose up -d spark-master 2>&1 && echo "Spark Master démarre. Attente de 5s ..." && sleep 3

# Démarrage de Spark History Server
docker compose up -d spark-history-server 2>&1 && echo "Spark History Serve démarre."

# ==== Vérification du cluster Spark ====
SPARK_STATUS=$(curl -s --connect-timeout 5 http://localhost:8080/json/)
if [ -z "$SPARK_STATUS" ]; then
    echo "Spark UI pas encore disponible, verification ignoree."
else
    WORKERS=$(echo "$SPARK_STATUS" | python3 -c "
import json,sys
d = json.load(sys.stdin)
workers = d.get('aliveworkers', 0)
memory = d.get('memoryperworker', 0)
print(f'  Workers actifs: {workers}, memoire par worker: {memory} MB')
" 2>/dev/null)
    echo "Cluster Spark operationnel."
    echo "$WORKERS"
fi

# ==== Démarrage du reste du stack (Prometheus, Grafana, cAdvisor, Trino...) ====
docker compose up -d 2>&1 | tail -15

# ==== URLs d'accès ====
export MY_IP=$(curl -4 -s --connect-timeout 3 ifconfig.me || hostname -I | awk '{print $1}')

echo "console seaweedFS: http://${MY_IP}:8888"
echo "console grafana: http://${MY_IP}:3000/dashboards (admin/admin)"
echo "console prometheus: http://${MY_IP}:9090/alerts"
echo "console presto: http://${MY_IP}:8088/ui/"
echo "console opensearch-dashboards: http://${MY_IP}:5601"
echo "console opensearch: http://${MY_IP}:9200"
echo "console spark-master: http://${MY_IP}:8080"
echo "console spark job actif: http://${MY_IP}:4040"
echo "console spark-history: http://${MY_IP}:18080"
echo "console C-Advisor: http://${MY_IP}:8081/containers/"

# ==== Vérification d'alertes Prometheus ====
sleep 5 && curl -s http://${MY_IP}:9090/api/v1/alerts | python3 -c "
import json,sys
data = json.load(sys.stdin)
alerts = data.get('data',{}).get('alerts',[])
if not alerts:
    print('pas d\'alertes dans Prometheus.')
else:
    for a in alerts:
        print(f\"{a['state']:10s} {a['labels'].get('alertname','?'):35s} {a['labels'].get('name', a['labels'].get('instance',''))}\") "
