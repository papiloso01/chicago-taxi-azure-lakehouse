# Azure infrastructure

Live resources were created manually, not through this repository. Before implementing Terraform,
record/export the resource settings, import existing resources and review the plan. Do not apply
fresh declarations over existing resources or migrate regions blindly. No infrastructure is
provisioned by the current CI. Use OIDC/managed identity when deployment automation is introduced.
