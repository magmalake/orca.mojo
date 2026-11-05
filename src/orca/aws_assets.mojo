"""The stack template and the project Dockerfile `orca aws` writes out:
deploy/aws/orca.yaml and deploy/project.Dockerfile, embedded by
scripts/embed-aws.py. Edit those, then run it; `pixi run test-aws-assets`
fails while this is out of date.
"""

comptime STACK_TEMPLATE: StaticString = """# orca on AWS with the ec2 runner (DESIGN.md, phase 5, PR 5b): one stack,
# so it's reviewed as one file and deleted as one stack.
#
# - a VPC with one public subnet in the Availability Zone given, and a
#   gateway endpoint for S3;
# - the control instance: the scheduler (ORCA_RUNNER=ec2), Postgres and
#   orca ui as containers, Caddy in front with a Let's Encrypt certificate;
# - what runs need: a bucket for the warehouse and logs, a repository for
#   the image, a role and profile for run instances, a security group they
#   share;
# - a bucket for the lake, mlake-public-<account>: the warehouse,
#   ORCA_WAREHOUSE, is in it; orca's own bucket keeps the logs. It's private
#   unless PublicData is true, when anyone may read it unsigned (the
#   project's resource control policy refuses other accounts' signed
#   requests), billed to the project at S3's egress price;
# - the console's hostname, served by the same Caddy from a container
#   named console on port 8080, once one runs;
# - a Budgets alert.
#
# orca ui is public for reading, as a demo is; its actions need a login
# (orca user add, in the scheduler's container). Nothing else accepts a
# connection from outside: Postgres takes the run instances' only, and the
# instances take each other's only.
AWSTemplateFormatVersion: "2010-09-09"
Description: orca with the ec2 runner, a control instance and runs on instances of their own

Parameters:
  AvailabilityZone:
    Type: AWS::EC2::AvailabilityZone::Name
    Default: us-east-2a
  ControlInstanceType:
    Type: String
    Default: t4g.large
  AllowedRunTypes:
    Type: CommaDelimitedList
    Default: "c7g.large,c7g.xlarge,c7g.2xlarge,c7g.4xlarge,t4g.medium,t4g.large"
    Description: The only instance types the scheduler may start
  Ami:
    Type: AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>
    Default: /aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id
    Description: arm64 Amazon Linux 2023 with Docker, for the control and run instances
  ImageTag:
    Type: String
    Default: latest
    Description: The tag of the orca image the control instance starts with (orca aws push pushes latest)
  ConsoleHostname:
    Type: String
    Default: libs.mlake.dev
    Description: The console app's hostname, an A record to PublicAddress; empty for none
  Hostname:
    Type: String
    Default: orca-demo.mlake.dev
    Description: The UI's public name, an A record to PublicAddress; it answers at <PublicAddress>.sslip.io too
  DbPassword:
    Type: String
    NoEcho: true
    MinLength: 16
    # It goes into a shell script and a URL as it is.
    AllowedPattern: "[A-Za-z0-9]+"
  BudgetEmail:
    Type: String
  PublicData:
    Type: String
    Default: "false"
    AllowedValues: ["true", "false"]
    Description: Whether anyone may read the lake's bucket unsigned; downloads are billed to the project
  BudgetDollars:
    Type: Number
    Default: 100

Conditions:
  Public: !Equals [!Ref PublicData, "true"]

Resources:
  Vpc:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: 10.42.0.0/16
      EnableDnsHostnames: true
      EnableDnsSupport: true
      Tags: [{Key: Name, Value: orca}]
  Subnet:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref Vpc
      CidrBlock: 10.42.0.0/20
      AvailabilityZone: !Ref AvailabilityZone
      MapPublicIpOnLaunch: true
      Tags: [{Key: Name, Value: orca}]
  Gateway:
    Type: AWS::EC2::InternetGateway
  GatewayAttachment:
    Type: AWS::EC2::VPCGatewayAttachment
    Properties: {VpcId: !Ref Vpc, InternetGatewayId: !Ref Gateway}
  Routes:
    Type: AWS::EC2::RouteTable
    Properties: {VpcId: !Ref Vpc}
  DefaultRoute:
    Type: AWS::EC2::Route
    DependsOn: GatewayAttachment
    Properties: {RouteTableId: !Ref Routes, DestinationCidrBlock: 0.0.0.0/0, GatewayId: !Ref Gateway}
  SubnetRoutes:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties: {SubnetId: !Ref Subnet, RouteTableId: !Ref Routes}
  # S3 traffic stays in the region, and costs nothing.
  S3Endpoint:
    Type: AWS::EC2::VPCEndpoint
    Properties:
      VpcId: !Ref Vpc
      ServiceName: !Sub com.amazonaws.${AWS::Region}.s3
      VpcEndpointType: Gateway
      RouteTableIds: [!Ref Routes]

  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub orca-${AWS::AccountId}-${AWS::Region}
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault: {SSEAlgorithm: AES256}
      LifecycleConfiguration:
        Rules:
          - {Id: logs, Status: Enabled, Prefix: logs/, ExpirationInDays: 30}
  # The lake's tables; anyone may read them when PublicData is true.
  # Account-level Block Public Access, if it's on, refuses the policy below.
  PublicBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub mlake-public-${AWS::AccountId}
      OwnershipControls:
        Rules: [{ObjectOwnership: BucketOwnerEnforced}]
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        IgnorePublicAcls: true
        BlockPublicPolicy: !If [Public, false, true]
        RestrictPublicBuckets: !If [Public, false, true]
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault: {SSEAlgorithm: AES256}
      # Browsers, such as DuckDB in a page, read it too.
      CorsConfiguration:
        CorsRules:
          - {AllowedMethods: [GET, HEAD], AllowedOrigins: ["*"], AllowedHeaders: ["*"], MaxAge: 3600}
  PublicBucketPolicy:
    Type: AWS::S3::BucketPolicy
    Condition: Public
    Properties:
      Bucket: !Ref PublicBucket
      PolicyDocument:
        Statement:
          - Effect: Allow
            Principal: "*"
            Action: s3:GetObject
            Resource: !Sub ${PublicBucket.Arn}/*
          - Effect: Allow
            Principal: "*"
            Action: s3:ListBucket
            Resource: !GetAtt PublicBucket.Arn

  Repository:
    Type: AWS::ECR::Repository
    Properties:
      RepositoryName: orca
      ImageScanningConfiguration: {ScanOnPush: true}
      LifecyclePolicy:
        LifecyclePolicyText: |
          {"rules": [{"rulePriority": 1, "description": "keep the last 5",
            "selection": {"tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 5},
            "action": {"type": "expire"}}]}

  RunGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: orca run instances, which talk to each other only
      VpcId: !Ref Vpc
  RunGroupSelf:
    Type: AWS::EC2::SecurityGroupIngress
    Properties:
      GroupId: !Ref RunGroup
      SourceSecurityGroupId: !Ref RunGroup
      IpProtocol: tcp
      FromPort: 0
      ToPort: 65535
  # The scheduler reads a scaling run's demand from its head (dista's port).
  RunGroupHead:
    Type: AWS::EC2::SecurityGroupIngress
    Properties:
      GroupId: !Ref RunGroup
      SourceSecurityGroupId: !Ref ControlGroup
      IpProtocol: tcp
      FromPort: 6390
      ToPort: 6390
  ControlGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: orca control instance, public for HTTPS only
      VpcId: !Ref Vpc
      SecurityGroupIngress:
        - {IpProtocol: tcp, FromPort: 80, ToPort: 80, CidrIp: 0.0.0.0/0}
        - {IpProtocol: tcp, FromPort: 443, ToPort: 443, CidrIp: 0.0.0.0/0}
        - {IpProtocol: tcp, FromPort: 5432, ToPort: 5432, SourceSecurityGroupId: !Ref RunGroup}

  RunRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Statement: [{Effect: Allow, Principal: {Service: ec2.amazonaws.com}, Action: sts:AssumeRole}]
      ManagedPolicyArns: [arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore]
      Policies:
        # Runs' secrets (orca register --secret), only those under orca/.
        - PolicyName: secrets
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action: secretsmanager:GetSecretValue
                Resource: !Sub arn:aws:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:orca/*
        - PolicyName: bucket-and-image
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action: [s3:GetObject, s3:PutObject, s3:DeleteObject]
                Resource: [!Sub "${Bucket.Arn}/*", !Sub "${PublicBucket.Arn}/*"]
              - Effect: Allow
                Action: s3:ListBucket
                Resource: [!GetAtt Bucket.Arn, !GetAtt PublicBucket.Arn]
              - Effect: Allow
                Action: [ecr:BatchGetImage, ecr:GetDownloadUrlForLayer]
                Resource: !GetAtt Repository.Arn
              - Effect: Allow
                Action: ecr:GetAuthorizationToken
                Resource: "*"
  RunProfile:
    Type: AWS::IAM::InstanceProfile
    Properties: {Roles: [!Ref RunRole]}

  ControlRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Statement: [{Effect: Allow, Principal: {Service: ec2.amazonaws.com}, Action: sts:AssumeRole}]
      ManagedPolicyArns: [arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore]
      Policies:
        - PolicyName: run-instances
          PolicyDocument:
            Statement:
              # Only instances tagged for orca, of the allowed types, from
              # the stack's AMI, in its subnet and group, with its profile.
              - Effect: Allow
                Action: ec2:RunInstances
                Resource: !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:instance/*
                Condition:
                  StringEquals: {ec2:InstanceType: !Ref AllowedRunTypes}
                  "Null": {"aws:RequestTag/orca.run": "false"}
              - Effect: Allow
                Action: ec2:RunInstances
                Resource:
                  - !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:volume/*
                  - !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:network-interface/*
                  - !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:subnet/${Subnet}
                  - !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:security-group/${RunGroup}
                  - !Sub arn:aws:ec2:${AWS::Region}::image/*
                  - !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:spot-instances-request/*
              - Effect: Allow
                Action: ec2:CreateTags
                Resource: !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:*/*
                Condition:
                  StringEquals: {ec2:CreateAction: RunInstances}
              - Effect: Allow
                Action: ec2:TerminateInstances
                Resource: !Sub arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:instance/*
                Condition:
                  "Null": {"aws:ResourceTag/orca.run": "false"}
              - Effect: Allow
                Action: ec2:DescribeInstances
                Resource: "*"
              - Effect: Allow
                Action: iam:PassRole
                Resource: !GetAtt RunRole.Arn
        - PolicyName: bucket-and-image
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action: [s3:GetObject, s3:PutObject, s3:DeleteObject]
                Resource: [!Sub "${Bucket.Arn}/*", !Sub "${PublicBucket.Arn}/*"]
              - Effect: Allow
                Action: s3:ListBucket
                Resource: [!GetAtt Bucket.Arn, !GetAtt PublicBucket.Arn]
              - Effect: Allow
                Action: [ecr:BatchGetImage, ecr:GetDownloadUrlForLayer]
                Resource: !GetAtt Repository.Arn
              - Effect: Allow
                Action: ecr:GetAuthorizationToken
                Resource: "*"
  ControlProfile:
    Type: AWS::IAM::InstanceProfile
    Properties: {Roles: [!Ref ControlRole]}

  ControlAddress:
    Type: AWS::EC2::EIP
    Properties: {Domain: vpc}
  Control:
    Type: AWS::EC2::Instance
    DependsOn: DefaultRoute
    Properties:
      ImageId: !Ref Ami
      InstanceType: !Ref ControlInstanceType
      SubnetId: !Ref Subnet
      SecurityGroupIds: [!Ref ControlGroup]
      IamInstanceProfile: !Ref ControlProfile
      MetadataOptions: {HttpTokens: required, HttpPutResponseHopLimit: 2}
      BlockDeviceMappings:
        - DeviceName: /dev/xvda
          Ebs: {VolumeSize: 40, VolumeType: gp3, Encrypted: true}
      Tags: [{Key: Name, Value: orca-control}]
      UserData:
        Fn::Base64: !Sub
          - |
            #!/bin/bash
            # orca's control instance: Postgres, the scheduler, the UI and
            # Caddy, as containers that restart with the instance.
            set -eux
            # Docker without ECS: this AMI's agent would look for a cluster.
            systemctl disable --now ecs || true
            systemctl enable --now docker
            command -v aws || dnf install -y awscli
            token=$(curl -sX PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
            md() { curl -s -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/$1; }
            ip=$(md local-ipv4)
            # The Elastic IP's sslip.io name works at once; the hostname once
            # its A record points here. Caddy gets a certificate for each.
            sslip="$(echo ${PublicIp} | tr . -).sslip.io"
            hosts="$sslip"
            [ -z "${Hostname}" ] || hosts="${Hostname}, $sslip"
            registry=${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com
            image=$registry/orca:${ImageTag}
            login="command -v aws >/dev/null || dnf install -y awscli; systemctl disable --now ecs || true; aws ecr get-login-password --region ${AWS::Region} | docker login --username AWS --password-stdin $registry"
            # The image is pushed after the stack is made, so wait for it.
            until sh -c "$login" && docker pull $image; do sleep 30; done
            docker network create orca || true
            mkdir -p /var/lib/orca/pg /var/lib/orca/caddy /var/lib/orca/stage
            docker run -d --name postgres --restart always --network orca -p 5432:5432 \\
              -e POSTGRES_USER=orca -e POSTGRES_DB=orca -e POSTGRES_PASSWORD='${DbPassword}' \\
              -v /var/lib/orca/pg:/var/lib/postgresql/data postgres:17-bookworm -c synchronous_commit=on
            until docker exec postgres pg_isready -U orca -d orca; do sleep 2; done
            cat > /etc/orca.env << EOF
            ORCA_DATABASE_URL=postgresql://orca:${DbPassword}@postgres:5432/orca
            ORCA_EC2_DATABASE_URL=postgresql://orca:${DbPassword}@$ip:5432/orca
            ORCA_RUNNER=ec2
            AWS_REGION=${AWS::Region}
            ORCA_EC2_AMI=${Ami}
            ORCA_EC2_SUBNET=${Subnet}
            ORCA_EC2_SECURITY_GROUP=${RunGroup}
            ORCA_EC2_PROFILE=${RunProfile}
            ORCA_EC2_PREAMBLE=$login
            ORCA_WAREHOUSE=s3://${PublicBucket}/warehouse
            ORCA_LOGS=s3://${Bucket}/logs
            ORCA_PUBLIC_WAREHOUSE=s3://${PublicBucket}/warehouse
            ORCA_RUN_ENV=ORCA_PUBLIC_WAREHOUSE
            ORCA_LOG_STAGE=/var/lib/orca/stage
            ORCA_CONTROL_SAMPLES=/var/lib/orca/stage/control.jsonl
            ORCA_CONTROL_TYPE=${ControlInstanceType}
            ORCA_UI_ADDR=0.0.0.0:8270
            ORCA_UI_LOGIN=required
            ORCA_UI_SECURE_COOKIES=1
            ORCA_UI_TRUST_PROXY=1
            EOF
            # orca aws register and orca aws app run these over SSM.
            cat > /usr/local/bin/orca-refresh << 'EOS'
            #!/bin/bash
            # orca-refresh [TAG]: pull the orca image at TAG (else the last
            # one) and run the scheduler and the UI from it again.
            set -eu
            repo=${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/orca
            [ $# -gt 0 ] && echo "$1" > /var/lib/orca/tag
            tag=$(cat /var/lib/orca/tag)
            aws ecr get-login-password --region ${AWS::Region} | docker login --username AWS --password-stdin "$(echo $repo | cut -d/ -f1)" > /dev/null
            docker pull -q "$repo:$tag"
            for role in scheduler ui; do
              docker rm -f $role > /dev/null 2>&1 || true
              docker run -d --name $role --restart always --network orca --env-file /etc/orca.env \\
                -v /var/lib/orca/stage:/var/lib/orca/stage "$repo:$tag" $role
            done
            # This instance's own samples, for the fleet page: the host's
            # processes and network, the last hour of lines kept in a file the
            # scheduler reads.
            docker rm -f sampler > /dev/null 2>&1 || true
            docker run -d --name sampler --restart always --pid host --network host \\
              -v /var/lib/orca/stage:/var/lib/orca/stage "$repo:$tag" \\
              sample --out /var/lib/orca/stage/control.jsonl --dirs /var/lib/orca/stage --keep 720
            EOS
            cat > /usr/local/bin/orca-app << 'EOS'
            #!/bin/bash
            # orca-app NAME HOST [PORT]: run the image's /opt/orca/bin/NAME as
            # an app beside the UI, at https://HOST through Caddy, with the
            # region, the warehouse and PORT (default 8080).
            set -eu
            name=$1
            host=$2
            port=8080
            [ $# -gt 2 ] && port=$3
            repo=${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/orca
            tag=$(cat /var/lib/orca/tag)
            aws ecr get-login-password --region ${AWS::Region} | docker login --username AWS --password-stdin "$(echo $repo | cut -d/ -f1)" > /dev/null
            docker pull -q "$repo:$tag"
            docker rm -f "$name" > /dev/null 2>&1 || true
            # An app reads the lake, not orca's database: it gets no password.
            grep -E '^(AWS_REGION|ORCA_WAREHOUSE|ORCA_PUBLIC_WAREHOUSE)=' /etc/orca.env > /var/lib/orca/app.env
            docker run -d --name "$name" --restart always --network orca --env-file /var/lib/orca/app.env \\
              -e PORT=$port --entrypoint /opt/orca/bin/$name "$repo:$tag"
            grep -q "^$host {" /var/lib/orca/Caddyfile \\
              || printf '%s {\\n  reverse_proxy %s:%s\\n}\\n' "$host" "$name" "$port" >> /var/lib/orca/Caddyfile
            docker exec caddy caddy reload --config /etc/caddy/Caddyfile
            EOS
            chmod +x /usr/local/bin/orca-refresh /usr/local/bin/orca-app
            echo ${ImageTag} > /var/lib/orca/tag
            orca-refresh
            cat > /var/lib/orca/Caddyfile << EOF
            $hosts {
              reverse_proxy ui:8270
            }
            EOF
            # The console, once a container named console runs on the network.
            [ -z "${ConsoleHostname}" ] || printf '%s {\\n  reverse_proxy console:8080\\n}\\n' "${ConsoleHostname}" >> /var/lib/orca/Caddyfile
            docker run -d --name caddy --restart always --network orca -p 80:80 -p 443:443 \\
              -v /var/lib/orca/Caddyfile:/etc/caddy/Caddyfile -v /var/lib/orca/caddy:/data caddy:2.10
            echo "orca ui at https://$sslip and https://${Hostname}"
          - {PublicIp: !Ref ControlAddress}
  ControlAddressAssociation:
    Type: AWS::EC2::EIPAssociation
    Properties: {InstanceId: !Ref Control, AllocationId: !GetAtt ControlAddress.AllocationId}

  Budget:
    Type: AWS::Budgets::Budget
    Properties:
      Budget:
        BudgetName: orca
        BudgetType: COST
        TimeUnit: MONTHLY
        BudgetLimit: {Amount: !Ref BudgetDollars, Unit: USD}
      NotificationsWithSubscribers:
        - Notification: {NotificationType: ACTUAL, ComparisonOperator: GREATER_THAN, Threshold: 80}
          Subscribers: [{SubscriptionType: EMAIL, Address: !Ref BudgetEmail}]
        - Notification: {NotificationType: FORECASTED, ComparisonOperator: GREATER_THAN, Threshold: 100}
          Subscribers: [{SubscriptionType: EMAIL, Address: !Ref BudgetEmail}]

Outputs:
  SslipUrl:
    Value: !Sub
      - https://${Dashed}.sslip.io
      - Dashed: !Join ["-", !Split [".", !Ref ControlAddress]]
  Repository:
    Value: !Sub ${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/orca
  Bucket:
    Value: !Ref Bucket
  PublicBucket:
    Description: The lake's bucket; with PublicData, read it unsigned, e.g. aws s3 ls --no-sign-request s3://<it>/
    Value: !Ref PublicBucket
  ControlInstance:
    Value: !Ref Control
  ConsoleHostname:
    Value: !Ref ConsoleHostname
  PublicAddress:
    Value: !Ref ControlAddress
"""

comptime PROJECT_DOCKERFILE: StaticString = """# The image `orca aws push` builds for a project that added orca with
# `pixi shelf add orca-mojo`: its own `orca` (bin/scheduler.mojo, the two
# lines calling orca.command), its apps (every other bin/*.mojo, at
# /opt/orca/bin/<name>, which `orca aws app` runs; `orca_maintain`, the
# snapshot expiry `orca expire` runs, is made for a project whose pipelines
# use orca.tables and that has no bin/orca_maintain.mojo of its own) and its
# pipelines (every
# pipelines/*.mojo, at /opt/orca/pipelines/<name>), with `src/` on the
# import path if the project has one. A project with a Dockerfile.orca of
# its own builds that instead.
#
# ORCA_SOURCE and ORCA_COMMIT say what the pipelines were built from, which
# each run records and orca ui links.
#
# Everything is built with line tables (-g1) and the image has
# llvm-symbolizer, so a crash's stack dump in a run's driver.log names each
# frame's function, file and line (orca.crash). The line tables, compressed,
# make a binary about half as big again (mojo-census's gharchive: 4.2 MB,
# 6.5 MB with them, 25 MB uncompressed) and leave its speed as it was.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS build
ARG ORCA_SOURCE=""
ARG ORCA_COMMIT=""
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \\
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/project
COPY pixi.toml pixi.lock ./
RUN pixi install --locked
COPY . .
RUN mkdir -p /out/bin /out/pipelines \\
    && pixi run sh -c 'inc=""; [ -d src ] && inc="-I src"; \\
        [ -f bin/scheduler.mojo ] || { echo "orca aws push: no bin/scheduler.mojo calling orca.command"; exit 1; }; \\
        for f in bin/*.mojo; do n=$(basename "$f" .mojo); [ "$n" = scheduler ] && n=orca; \\
            mojo build -g1 $inc "$f" -o "/out/bin/$n" || exit 1; done; \\
        if [ ! -f bin/orca_maintain.mojo ] && grep -rqs "orca.tables" pipelines src; then \\
            printf "from orca.maintain import maintain_main\\n\\n\\ndef main() raises:\\n    maintain_main()\\n" > /tmp/orca_maintain.mojo; \\
            mojo build -g1 $inc /tmp/orca_maintain.mojo -o /out/bin/orca_maintain || exit 1; fi; \\
        for p in pipelines/*.mojo; do [ -f "$p" ] || continue; n=$(basename "$p" .mojo); \\
            mojo build -g1 $inc -D ORCA_SOURCE='"${ORCA_SOURCE}"' -D ORCA_COMMIT='"${ORCA_COMMIT}"' \\
                "$p" -o "/out/pipelines/$n" || exit 1; done; \\
        if ls /out/pipelines/* > /dev/null 2>&1; then /out/bin/orca locate /out/pipelines/* --root .; fi'
# The line tables compressed, which llvm-symbolizer reads as they are.
RUN for b in /out/bin/* /out/pipelines/*; do [ -x "$b" ] || continue; \\
        objcopy --compress-debug-sections=zlib "$b" || exit 1; done
# Only the pixi environment's libraries the binaries link or dlopen: libpq
# (postgres), flare's shims (orca ui, apps), and every tin's shim, such as
# objectstore's and the codecs' (zstd, for Iceberg writes); and
# llvm-symbolizer, which Mojo's runtime runs to name a crash's frames, with
# what it links.
RUN env=/src/project/.pixi/envs/default && mkdir -p /out/lib /out/libexec \\
    && cp -L $env/bin/llvm-symbolizer /out/libexec/ \\
    && loaded=$(ls $env/lib/libpq.so* $env/lib/libflare_*.so $env/lib/lib*mojo*.so 2>/dev/null) \\
    && ldd /out/bin/* /out/pipelines/* /out/libexec/* $loaded 2> /dev/null | awk -v e="$env" '$3 ~ "^"e {print $3}' \\
        | sort -u | xargs -r cp -L -t /out/lib \\
    && cp -L -t /out/lib $loaded

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \\
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/pipelines /opt/orca/pipelines
COPY --from=build /out/libexec /opt/orca/libexec
ENV CONDA_PREFIX=/opt/orca LD_LIBRARY_PATH=/opt/orca/lib PATH=/opt/orca/bin:/opt/orca/pipelines:/usr/bin:/bin \\
    LLVM_SYMBOLIZER_PATH=/opt/orca/libexec/llvm-symbolizer
ENTRYPOINT ["/opt/orca/bin/orca"]
"""
