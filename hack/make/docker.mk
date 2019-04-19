ifndef INCLUDE_DOCKER # Do not load this include more than once.
export INCLUDE_DOCKER := true

# Recipes that require Docker should use DOCKER_SOCK as an order-only
# dependency. If the DOCKER_SOCK does not exist then the Docker server
# will be started inside the container. If DOCKER_SOCK *does* exist then
# DOCKER_SOCK_FROM_HOST is set to "true" to indicate that the Docker
# socket was provided by the host system.
DOCKER_SOCK := /var/run/docker.sock

ifeq (,$(strip $(wildcard $(DOCKER_SOCK))))

$(DOCKER_SOCK):
	@printf "starting docker..."
	@service docker start >/dev/null 2>&1 || true;
	@i=0 && while ! docker ps -aq 2>/dev/null; do \
	  exit_code="$${?}"; \
	  if [ "$${i}" -ge "300" ]; then \
	    echo "failed" 1>&2; \
	    [ -f /var/log/docker.log ] && cat /var/log/docker.log 1>&2; \
	    exit "$${exit_code}"; \
	  fi; \
	  sleep 1 && printf "." && i=$$((i+1)); \
	done; \
	echo "success"

else

# If the Docker socket already exists then indicate the socket was
# provided by the host system. However, do not redefine this value.
# In the case of the project's root Makefile invoking other Makefiles,
# the Docker server could have been started in the container and thus
# DOCKER_SOCK_FROM_HOST should not be modified.
export DOCKER_SOCK_FROM_HOST ?= true

endif

endif # ifndef INCLUDE_DOCKER