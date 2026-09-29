#!/usr/bin/env bash
# -------------------------------------------------------------------------------
# MesoDB Release script
#         Usage: ./release.sh <new-version>
#       Example: ./release.sh 0.1.0
#   Description: This script automates the release process for MesoDB.
#
# It performs the following steps:
#     1. Validates the input version format.
#     2. Updates the version in Cargo.toml (Workspace & Dependencies).
#     3. Commits the version bump.
#     4. Creates a git tag for the new version.
#     5. Pushes the commit and tag to GitHub (triggering cargo-dist).
#     6. Publishes the 'mesodb-core' crate to crates.io.
#     7. Waits for 'mesodb-core' to be available on crates.io.
#     8. Publishes the 'mesodb-server' crate to crates.io.
#     9. Waits for 'mesodb-server' to be available on crates.io.
#    10. Publishes the 'mesodb-bench' crate to crates.io.
# -------------------------------------------------------------------------------

BT_LOGO=$(cat <<'BT_TEXT'
  __  __                 ___  ___
 |  \/  | ___  ___  ___ |   \| _ )
 | |\/| |/ -_)(_-< / _ \| |) | _ \
 |_|  |_|\___|/__/ \___/|___/|___/

BT_TEXT
)

BOLD='\033[1m'
ITAL='\033[3m'
BLUE='\033[0;34m'
RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m'

ERROR="${RED}[ERROR]${NC}"
HINT="${ORANGE}[HINT]${NC}"
INFO="${BLUE}[INFO]${NC}"
SUCCESS="${GREEN}[OK]${NC}"

echo -e "${BLUE}${BT_LOGO}${NC}\n"

show_spinner() {
    local pid=$1
    local msg=$2
    local delay=0.1
    local spinstr='|/-\'

    tput civis
    trap "tput cnorm; echo; exit 1" INT TERM

    while kill -0 "$pid" 2>/dev/null; do
        local temp=${spinstr#?}
        printf "\r ${BLUE}[%c]${NC} %b" "$spinstr" "$msg"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
    done

    printf "\r\033[K"
    tput cnorm
    trap - INT TERM
}

#### ________________________________________________________ CONFIGURE RELEASE

script_dir=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
cd "$script_dir"

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo -e "      ${BOLD}Usage:${NC} $0 <new-version>"
    echo -e "    ${BOLD}Example:${NC} $0 0.1.0"
    echo -e "\n$ERROR Missing required version argument."
    exit 1
fi

if [[ ! "$1" =~ ^(v)?[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
    echo -e "$ERROR Version must be in the format X.Y.Z (e.g., 0.1.0)"
    exit 1
fi

# Check if the git working directory is clean
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo -e "$ERROR Git working directory is not clean. Commit or stash changes first."
    exit 1
fi

OLD_VERSION=$(grep '^version = ' Cargo.toml | head -n 1 | sed -E 's/version = "(.*)"/\1/')
NEW_VERSION="${1#v}"
TAG="v$NEW_VERSION"

echo -e "${INFO} Found the following:"
echo -e "  ${BLUE}OLD_VERSION:${NC}$OLD_VERSION"
echo -e "  ${BLUE}NEW_VERSION:${NC}$NEW_VERSION"
echo -e "          ${BLUE}TAG:${NC}$TAG"

CONFIG_MSG=$(echo -e "\n${ORANGE}Continue?${NC}(${GREEN}y${NC}/${RED}n${NC}): ")
read -p "$CONFIG_MSG " -n 1 -r; echo; [[ $REPLY =~ ^[Yy]$ ]] || exit 1

#### ________________________________________________________ START RELEASE

echo -e "\n${INFO} 0/16. ${BOLD}Starting release:${NC} ${BLUE}${NEW_VERSION}${NC}"

# Update Cargo.toml with the new version (Workspace & Dependencies)
echo -e "$INFO 1/16. ${BOLD}Cargo.toml:${NC} ${BLUE}${OLD_VERSION}${NC} --> ${BLUE}${NEW_VERSION}${NC}"
sed -i.bak -e "s/^version = \".*\"/version = \"$NEW_VERSION\"/" Cargo.toml
sed -i.bak -E "s/^(mesodb-core[[:space:]]*=.*version[[:space:]]*=[[:space:]]*\")[^\"]+(\".*)$/\1$NEW_VERSION\2/" Cargo.toml
sed -i.bak -E "s/^(mesodb-server[[:space:]]*=.*version[[:space:]]*=[[:space:]]*\")[^\"]+(\".*)$/\1$NEW_VERSION\2/" Cargo.toml
rm Cargo.toml.bak

echo -e "$INFO 2/16. ${BOLD}Cargo.lock:${NC} syncing with Cargo.toml"
cargo check --quiet

echo -e "$INFO 3/16. ${BOLD}Committing version bump${NC}"
git add Cargo.toml Cargo.lock
git commit -m "chore: bump version to $NEW_VERSION"

echo -e "$INFO 4/16. ${BOLD}Creating tag $TAG${NC}"
git tag "$TAG"

echo -e "$INFO 5/16. ${BOLD}Pushing to GitHub (Triggering cargo-dist)${NC}"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
git push origin "$CURRENT_BRANCH"
git push origin "$TAG"

# --- PUBLISH mesodb-core ---
echo -e "$INFO 6/16. ${BOLD}mesodb-core (crate):${NC} Dry run"
cargo publish -p mesodb-core --dry-run
echo -e "$INFO 7/16. ${BOLD}mesodb-core (crate):${NC} Publishing "
cargo publish -p mesodb-core

echo -e "$INFO 8/16. ${BOLD}Waiting for 'mesodb-core' to be ready on crates.io${NC}"
MAX_RETRIES=30
for ((i=1; i<=MAX_RETRIES; i++)); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "https://crates.io/api/v1/crates/mesodb-core/$NEW_VERSION")
    if [ "$HTTP_CODE" -eq 200 ]; then
        echo -e "${SUCCESS} ${BLUE}mesodb-core v$NEW_VERSION${NC} is live! (Found on attempt $i/$MAX_RETRIES)"
        break
    fi
    STATUS_MSG="Attempt ${BOLD}${i}/${MAX_RETRIES}${NC} | Status: ${RED}${HTTP_CODE} (Not found)${NC} | Waiting 10s..."
    sleep 10 &
    show_spinner $! "$STATUS_MSG"

    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo -e "\n$ERROR Timed out waiting for crates.io to index mesodb-core."
        exit 1
    fi
done

echo -e "$INFO 9/16. ${BOLD}Syncing local Cargo registry${NC}"
cargo update -p mesodb-core || cargo search mesodb-core --limit 1 > /dev/null

# --- PUBLISH mesodb-server ---
echo -e "$INFO 10/16. ${BOLD}mesodb-server (crate):${NC} Dry run"
cargo publish -p mesodb-server --dry-run

echo -e "$INFO 11/16. ${BOLD}mesodb-server (crate):${NC} Publishing"
cargo publish -p mesodb-server

echo -e "$INFO 12/16. ${BOLD}Waiting for 'mesodb-server' to be ready on crates.io${NC}"
for ((i=1; i<=MAX_RETRIES; i++)); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "https://crates.io/api/v1/crates/mesodb-server/$NEW_VERSION")
    if [ "$HTTP_CODE" -eq 200 ]; then
        echo -e "${SUCCESS} ${BLUE}mesodb-server v$NEW_VERSION${NC} is live! (Found on attempt $i/$MAX_RETRIES)"
        break
    fi
    STATUS_MSG="Attempt ${BOLD}${i}/${MAX_RETRIES}${NC} | Status: ${RED}${HTTP_CODE} (Not found)${NC} | Waiting 10s..."
    sleep 10 &
    show_spinner $! "$STATUS_MSG"

    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo -e "\n$ERROR Timed out waiting for crates.io to index mesodb-server."
        exit 1
    fi
done

echo -e "$INFO 13/16. ${BOLD}Syncing local Cargo registry${NC}"
cargo update -p mesodb-server || cargo search mesodb-server --limit 1 > /dev/null

# --- PUBLISH mesodb-bench ---
echo -e "$INFO 14/16. ${BOLD}mesodb-bench (crate):${NC} Dry run"
cargo publish -p mesodb-bench --dry-run

echo -e "$INFO 15/16. ${BOLD}mesodb-bench (crate):${NC} Publishing"
cargo publish -p mesodb-bench

echo -e "$INFO 16/16. ${SUCCESS} Successfully deployed and published version $NEW_VERSION!"
