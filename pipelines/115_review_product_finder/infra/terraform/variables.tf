variable "organization_name" {
  type = string
}

variable "account_name" {
  type = string
}

variable "user" {
  type = string
}

variable "private_key" {
  type      = string
  sensitive = true
}

variable "monthly_credit_quota" {
  type    = number
  default = 10
}
