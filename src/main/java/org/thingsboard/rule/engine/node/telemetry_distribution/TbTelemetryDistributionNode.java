/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.telemetry_distribution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.dao.relation.RelationService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;

@RuleNode(
        type = ComponentType.ACTION,
        name = "Telemetry distribution",
        configClazz = TbTelemetryDistributionNodeConfiguration.class,
        nodeDescription = "Distributes the telemetry message of storage room to all the products that are included inside it.",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbTransformationNodeSumConfig")
public class TbTelemetryDistributionNode implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    TbTelemetryDistributionNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) {
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        try {
            RelationService relationService = ctx.getRelationService();
            RuleEngineTelemetryService ruleEngineTelemetryService = ctx.getTelemetryService();

            TenantId tenantId = ctx.getTenantId();
            EntityId fromLocationId = msg.getOriginator();

            List<EntityRelation> entityRelationList = relationService.findByFromAndType(tenantId, fromLocationId, EntityRelation.CONTAINS_TYPE, RelationTypeGroup.COMMON);

            for (EntityRelation entityRelation : entityRelationList) {
                EntityId productId = entityRelation.getTo();
                JsonNode jsonNode = mapper.readTree(msg.getData());
                ArrayList<TsKvEntry> tsKvEntryArrayList = new ArrayList<>();
                for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> kv = it.next();
                    if (kv.getKey().equals("temperature") || kv.getKey().equals("humidity")) {
                        tsKvEntryArrayList.add(new BasicTsKvEntry(msg.getTs(), new JsonDataEntry(kv.getKey(), kv.getValue().asText())));
                    }
                }

                ruleEngineTelemetryService.saveLatestAndNotify(tenantId, productId, tsKvEntryArrayList, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(@Nullable Void unused) {
                        System.out.println("SUCCESSFULLY SAVED LATEST TELEMETRY");
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        System.out.println("FAILED TO SAVE LATEST TELEMETRY");
                    }
                });

                ruleEngineTelemetryService.saveAndNotify(tenantId, productId, tsKvEntryArrayList, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(@Nullable Void unused) {
                        System.out.println("SUCCESSFULLY SAVED TELEMETRY");
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        System.out.println("FAILED TO SAVE TELEMETRY");
                    }
                });
            }

            ctx.tellNext(msg, SUCCESS);
        } catch (JsonProcessingException ex) {
            ctx.tellFailure(msg, new TbNodeException("Failed to parse telemetry message : Invalid JSON format."));
        }
    }

    @Override
    public void destroy() {
    }
}
