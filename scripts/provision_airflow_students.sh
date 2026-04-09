#!/usr/bin/env bash
# Создаёт 30 учёток Airflow (роль Admin). Идемпотентно.
set -euo pipefail

COUNT="${WORKSHOP_STUDENT_COUNT:-30}"

echo "==> Workshop: provisioning ${COUNT} student Airflow accounts (role Admin)…"

for i in $(seq 1 "${COUNT}"); do
  sid="$(printf '%02d' "$i")"
  uname="workshop_student_${sid}"
  pwd="workshop_stu${sid}"
  if airflow users create \
    --username "${uname}" \
    --firstname Student \
    --lastname "${sid}" \
    --role Admin \
    --email "${uname}@workshop.local" \
    --password "${pwd}"; then
    echo "    created ${uname}"
  else
    echo "    skip ${uname} (likely already exists)"
  fi
done

echo "==> Airflow student provisioning finished OK."
