import subprocess


class DVCArtifactHandler:
    @staticmethod
    def get_local_artifact(artifact_path):
        """Ensure a DVC artifact is cached and return the local path."""
        try:
            subprocess.check_output(["dvc", "pull", artifact_path])
        except subprocess.CalledProcessError:
            raise Exception(
                "Attempt to pull artifact '%s' failed, check that the path is accurate"
                % artifact_path
            )
        return artifact_path
